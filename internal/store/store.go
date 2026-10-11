package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
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
		if seen.Get(key) != nil {
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

// Prune keeps pending markers indefinitely; only delivered markers expire after 30 days.
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
