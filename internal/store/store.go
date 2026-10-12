package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"tgntf/internal/trade"
)

var pendingBucket = []byte("pending")
var seenBucket = []byte("seen")

type Store struct{ db *bolt.DB }
type Item struct {
	Sequence uint64
	Fill     trade.Fill
}

type OrderItem struct {
	Order trade.Order
	Items []Item
}

func closedKey(orderKey string) []byte { return []byte(orderKey + ":closed") }

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{pendingBucket, seenBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Enqueue commits the fill and deduplication marker atomically before any network delivery.
func (s *Store) Enqueue(fill trade.Fill) (bool, error) {
	encoded, err := json.Marshal(fill)
	if err != nil {
		return false, err
	}
	added := false
	err = s.db.Update(func(tx *bolt.Tx) error {
		seen := tx.Bucket(seenBucket)
		key := []byte(fill.Key())
		if seen.Get(key) != nil || seen.Get(closedKey(fill.OrderKey())) != nil {
			return nil
		}
		pending := tx.Bucket(pendingBucket)
		seq, err := pending.NextSequence()
		if err != nil {
			return err
		}
		if err := pending.Put(number(seq), encoded); err != nil {
			return err
		}
		if err := seen.Put(key, number(0)); err != nil {
			return err
		}
		added = true
		return nil
	})
	return added, err
}

// NextOrder skips open orders so an unfinished order cannot block other notifications.
// Keeping the original fills also makes queues from earlier versions usable as-is.
func (s *Store) NextOrder() (*OrderItem, error) {
	var next *OrderItem
	err := s.db.View(func(tx *bolt.Tx) error {
		orders := make(map[string]*OrderItem)
		var ordered []*OrderItem
		cursor := tx.Bucket(pendingBucket).Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var fill trade.Fill
			if err := json.Unmarshal(value, &fill); err != nil {
				return errors.New("invalid persisted fill")
			}
			order := orders[fill.OrderKey()]
			if order == nil {
				order = &OrderItem{}
				orders[fill.OrderKey()] = order
				ordered = append(ordered, order)
			}
			if err := order.Order.Add(fill); err != nil {
				return fmt.Errorf("aggregate persisted order: %w", err)
			}
			order.Items = append(order.Items, Item{Sequence: binary.BigEndian.Uint64(key), Fill: fill})
		}
		for _, order := range ordered {
			if order.Order.Fill.Terminal() {
				next = order
				break
			}
		}
		return nil
	})
	return next, err
}

// AckOrder acknowledges every included execution and the order in one transaction.
func (s *Store) AckOrder(item OrderItem, now time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		pending, seen := tx.Bucket(pendingBucket), tx.Bucket(seenBucket)
		sequences := make(map[uint64]bool, len(item.Items))
		for _, fill := range item.Items {
			sequences[fill.Sequence] = true
		}
		// Do not discard a distinct execution received while Send was in progress.
		cursor := pending.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var fill trade.Fill
			if err := json.Unmarshal(value, &fill); err != nil {
				return errors.New("invalid persisted fill")
			}
			if fill.OrderKey() == item.Order.Fill.OrderKey() && !sequences[binary.BigEndian.Uint64(key)] {
				return errors.New("order changed during delivery; notification retained")
			}
		}
		for _, fill := range item.Items {
			if err := pending.Delete(number(fill.Sequence)); err != nil {
				return err
			}
			if err := seen.Put([]byte(fill.Fill.Key()), number(uint64(now.UnixMilli()))); err != nil {
				return err
			}
		}
		return seen.Put(closedKey(item.Order.Fill.OrderKey()), number(uint64(now.UnixMilli())))
	})
}

// DiscardExcluded also removes open orders left in the queue by earlier configuration.
func (s *Store) DiscardExcluded(excludes func(string) bool, now time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(pendingBucket).Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var fill trade.Fill
			if err := json.Unmarshal(value, &fill); err != nil {
				return errors.New("invalid persisted fill")
			}
			if !excludes(fill.Symbol) {
				continue
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
			if err := tx.Bucket(seenBucket).Put([]byte(fill.Key()), number(uint64(now.UnixMilli()))); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Next() (*Item, error) {
	var item *Item
	err := s.db.View(func(tx *bolt.Tx) error {
		key, value := tx.Bucket(pendingBucket).Cursor().First()
		if key == nil {
			return nil
		}
		var fill trade.Fill
		if err := json.Unmarshal(value, &fill); err != nil {
			return errors.New("invalid persisted fill")
		}
		item = &Item{Sequence: binary.BigEndian.Uint64(key), Fill: fill}
		return nil
	})
	return item, err
}

func (s *Store) Ack(item Item, now time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(pendingBucket).Delete(number(item.Sequence)); err != nil {
			return err
		}
		return tx.Bucket(seenBucket).Put([]byte(item.Fill.Key()), number(uint64(now.UnixMilli())))
	})
}

// Prune keeps pending markers indefinitely; acknowledged markers expire after 30 days.
func (s *Store) Prune(now time.Time) error {
	cutoff := uint64(now.Add(-30 * 24 * time.Hour).UnixMilli())
	return s.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(seenBucket).Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			delivered := binary.BigEndian.Uint64(v)
			if delivered != 0 && delivered < cutoff {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func number(value uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return b
}
