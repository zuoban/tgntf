# tgntf：币安成交 Telegram 通知

Go 实现，监听一个币安账户的**现货 + U 本位合约**成交，通过 Telegram Bot 向指定聊天发送中文通知。使用 Docker Compose 部署。

- 现货：WebSocket API `userDataStream.subscribe.signature`，使用 HMAC SHA-256 签名。
- 合约：`/fapi/v1/listenKey` + 私有 WebSocket，每 30 分钟续期。
- 按订单 ID 汇总多笔成交，订单完全成交后发送一条通知；部分成交后撤单或过期，默认也发送已成交部分的汇总。无成交的挂单、撤单和账户余额变化不会触发通知。
- 可通过 `EXCLUDED_SYMBOLS` 排除指定交易对，现货和合约均不发送这些交易对的通知。
- 通知包含买卖方向、整单成交均价/量/额、累计手续费、成交笔数、订单 ID 和时间；合约另含持仓方向、仅减仓标志及累计已实现盈亏。
- 两个市场独立连接，心跳检测、指数退避重连，连接在 24 小时限制前轮换。
- bbolt 持久化通知队列和去重记录；Telegram 网络错误、5xx、429 自动重试并遵守 `retry_after`。
- 容器以普通用户运行，持久化数据卷，健康检查、日志轮转和优雅退出。

## 1. 准备 Telegram

1. 在 Telegram 向 [@BotFather](https://t.me/BotFather) 发送 `/newbot`，取得 Bot Token。
2. 用接收通知的账号打开机器人并点击 **Start**，或发送 `/start`。群组通知需要将机器人加入群组并允许发消息。
3. 查看该机器人的 `getUpdates`，从目标聊天的 `message.chat.id` 获取数字 ID。私聊通常为正数，群组通常为负数。服务不消费更新，也不响应聊天命令。

可以在终端中执行以下命令读取更新，Token 放在环境变量中：

```sh
read -r -s TELEGRAM_BOT_TOKEN
export TELEGRAM_BOT_TOKEN
curl -fsS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates"
```

如果返回空的 `result`，先给机器人发一条新消息再执行；已有其他程序消费更新或设置 webhook 的机器人应使用独立 Bot Token。

## 2. 配置环境变量

```sh
cp .env.example .env
chmod 600 .env
```

编辑 `.env`：

```dotenv
BINANCE_API_KEY=你的币安APIKey
BINANCE_SECRET_KEY=你的币安HMACSecretKey
TELEGRAM_BOT_TOKEN=你的TelegramBotToken
TELEGRAM_CHAT_ID=你的数字ChatID
ENABLE_SPOT=true
ENABLE_FUTURES=true
NOTIFY_PARTIAL_FILLS=true
EXCLUDED_SYMBOLS=
NOTIFY_TIMEZONE=Asia/Shanghai
```

币安密钥需要读取账户数据的权限，并能访问两个市场的用户数据流。支持普通现货账户及标准 U 本位合约账户，统一/组合保证金账户的专用数据流不在本项目范围内。程序没有下单、撤单或提现接口。建议使用专用 API Key 和服务器 IP 白名单。实际密钥仅在运行时通过环境变量注入；`.env` 不进入 Git 或 Docker 构建上下文。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `BINANCE_API_KEY` | 必填 | 币安 HMAC API Key |
| `BINANCE_SECRET_KEY` | 必填 | 币安 HMAC Secret Key（不是 RSA/Ed25519 私钥） |
| `TELEGRAM_BOT_TOKEN` | 必填 | BotFather 发放的 Token |
| `TELEGRAM_CHAT_ID` | 必填 | 固定接收聊天的数字 ID，不能为 0 |
| `ENABLE_SPOT` | `true` | 现货监听开关 |
| `ENABLE_FUTURES` | `true` | U 本位合约监听开关 |
| `NOTIFY_PARTIAL_FILLS` | `true` | `true` 也汇总部分成交后撤单/过期的订单；`false` 只通知完全成交的订单。两种设置都会累计所有成交，进行中的订单暂不通知 |
| `EXCLUDED_SYMBOLS` | 空 | 排除通知的完整交易对，英文逗号分隔，如 `BTCUSDT,ETHUSDT`；现货和合约共同生效，忽略大小写及两侧空白 |
| `NOTIFY_TIMEZONE` | `Asia/Shanghai` | 通知显示时区 |
| `STATE_PATH` | `data/notifications.db` | 本地队列路径；Compose 固定为 `/data/notifications.db` |
| `HEALTH_ADDR` | `127.0.0.1:8080` | 容器内部健康服务；Compose 固定为此值 |
| `BINANCE_SPOT_REST_URL` | `https://api.binance.com` | 现货服务器时间接口 |
| `BINANCE_SPOT_WS_URL` | `wss://ws-api.binance.com:443/ws-api/v3` | 现货 WebSocket API |
| `BINANCE_FUTURES_REST_URL` | `https://fapi.binance.com` | 合约 listenKey REST API |
| `BINANCE_FUTURES_WS_URL` | `wss://fstream.binance.com/private/ws` | 合约私有 WebSocket 基址，程序会追加 listenKey |
| `TELEGRAM_API_URL` | `https://api.telegram.org` | Telegram Bot API 基址 |
| `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` | 空 | HTTP(S)/WebSocket 代理；地址中的 `localhost` 指容器自身 |

至少开启一个市场。所有远程地址必须为 HTTPS/WSS。可以替换为官方测试网接口，但测试网密钥不能用于主网，且现货与合约测试网凭据一般不同，本程序每个实例使用一组币安密钥；分别测试时关闭另一市场。

例如设置 `EXCLUDED_SYMBOLS=BTCUSDT,ETHUSDT` 后，这两个交易对的成交不再通知，其他交易对正常通知。交易对按完整名称匹配，不支持通配符；留空表示不排除任何交易对。更新后重启程序或重建容器生效，队列中尚未发送的排除交易对通知也会移除，取消排除不会补发已跳过的通知。

## 3. 使用公开容器镜像

GitHub 仓库：[zuoban/tgntf](https://github.com/zuoban/tgntf)。公开镜像 `ghcr.io/zuoban/tgntf:0.1.2` 支持 `linux/amd64` 和 `linux/arm64`，公开后可匿名拉取，不需要 `docker login`。

克隆仓库并按上文填写 `.env`，然后直接拉取运行：

```sh
docker compose pull
docker compose up -d --no-build
docker compose logs -f --tail=100
```

`.env` 的 `TGNTF_TAG` 默认是 `0.1.2`，也可以设置为 `v0.1.2` 或 `latest`。建议生产使用具体版本。新版镜像发布后，修改版本并再次执行 `pull` 和 `up` 即可更新，数据卷会继续保留。

从 v0.1.1 升级到 v0.1.2 后，通知改为订单结束时汇总发送。`NOTIFY_PARTIAL_FILLS=true` 表示也通知部分成交后撤单或过期的订单；`false` 只通知完全成交的订单，两者都会累计所有实际成交。旧版尚未发送的队列可以直接使用，无需清空数据卷。

## 从源码构建部署

```sh
docker compose build
docker compose run --rm tgntf -check-config
docker compose up -d
docker compose logs -f --tail=100
```

`-check-config` 只校验配置格式，不访问币安或 Telegram，也不验证密钥权限。运行后看到 `spot` 的 `subscribed` 和 `futures` 的 `connected` 日志即表示已建立监听。订单汇总成功投递后会看到 `Order summary delivered`。

```sh
docker compose ps
docker compose exec tgntf /usr/local/bin/tgntf -healthcheck
docker compose down
```

`down` 保留通知数据卷；`down -v` 会删除队列与去重记录。更新配置后用 `docker compose up -d --force-recreate` 重建容器。只运行一个实例，勿共享同一数据卷或对同一账户并行部署多个通知实例；合约 listenKey 属于账户，程序退出时不主动删除，以免影响其他监听程序。

容器不发布任何端口。`/healthz` 表示进程存活，`/readyz` 只有所有启用的数据流已连接、最近通知没有投递失败时返回 200。Docker 健康检查使用 `/readyz`；变为 unhealthy 不会自动触发 Docker 重启，应结合监控查看日志。

如果币安凭据被拒绝，或 Telegram 返回不可重试错误（如聊天 ID 错误、Bot 被屏蔽、Token 无效），进程退出并保留未投递消息。修正配置后重启即可。Compose 的重启策略会自动重启异常退出的进程，因此持续配置错误会持续出现在日志中。

## 本地开发

需要 Go 1.26+。本地运行也从环境变量读取配置，不自动加载 `.env`：

```sh
set -a
. ./.env
set +a
go run ./cmd/tgntf -check-config
go run ./cmd/tgntf
```

```sh
go test -race ./...
go vet ./...
go build -o bin/tgntf ./cmd/tgntf
```

测试全部使用本地模拟服务和临时数据库，无需密钥，也不会给真实 Telegram 聊天发送消息。覆盖现货签名与时间同步、合约续期/过期、WebSocket ping/pong 和断线重连、精确大小写字段解析、订单汇总、部分成交后撤单、多币种手续费、并发去重、重启恢复、Telegram 重试及两个市场完整通知链路。

## 通知示例

```text
币安订单汇总 · U 本位合约
ETHUSDT | 卖出 🔴
状态：完全成交 · MARKET
成交均价：3000
成交量：1
成交额：3000（计价币）
手续费：1.2 USDT
流动性：Taker
持仓方向：LONG
仅减仓：true
已实现盈亏：5（结算币，未扣手续费）
成交笔数：2
订单 ID：11
首笔成交时间：2026-10-12 12:40:38.697 +08:00
订单结束时间：2026-10-12 12:40:38.698 +08:00
```

## 投递语义与边界

- 每笔成交以 `市场 + 交易对 + 订单 ID + 成交 ID` 去重，再以 `市场 + 交易对 + 订单 ID` 汇总；不同市场或交易对的相同订单 ID 不会混在一起。整单成功发送后一起确认全部成交，并保留订单去重标记，避免重复发送同一订单。
- 订单状态为 `FILLED` 时发送一条汇总。`NOTIFY_PARTIAL_FILLS=true` 也会在 `CANCELED`、`EXPIRED` 或 `EXPIRED_IN_MATCH` 时汇总已成交部分；`false` 会清除这些订单而不通知。部分成交期间持续落盘，未结束的订单不会阻塞其他已经结束的订单，程序重启后继续累计。
- 已发送或因排除配置而移除的队列记录，其去重标记保留 30 天；其他待发送记录一直保留到订单结束并成功投递。新版可直接读取旧版尚未发送的成交队列；已经发送的旧通知不会撤回。
- 非排除交易对的成交先落盘再发送，Telegram 故障期间持续接收并排队。长时间故障或长期未结束的部分成交订单会占用更多磁盘；磁盘写入失败时程序退出，无法落盘的事件没有保证。
- Telegram 不提供发送消息的幂等键。若消息已经送达，但 HTTP 响应丢失或进程在确认入库前退出，重试可能重复发送。这是至少一次投递语义，不能保证严格恰好一次。
- **本项目监听实时成交，不自动补发历史成交。启动前、停机期间以及 WebSocket 断线窗口内发生的成交可能遗漏。** 自动重连不能恢复未收到的事件。如需历史补偿，应另加指定交易对的 REST 成交历史对账。
- 接收普通现货 `executionReport` 和标准合约 `ORDER_TRADE_UPDATE` 的 `TRADE` 成交，并使用已经有成交量的订单结束事件收尾；结束事件不计入成交笔数，也不重复累计费用。不会把 `TRADE_LITE` 重复推送或 `CALCULATED` 强平状态事件视为普通成交记录。币本位合约、杠杆、Convert 等其他产品需独立数据流。
- 金额/数量使用十进制定点运算，避免 float64 精度损失；现货累计均价最多保留 16 位小数。成交量与均价使用币安累计值，现货成交额使用币安累计成交额，合约成交额累加每笔成交额。手续费按币种分别累计，已实现盈亏累加每笔结算币盈亏且不扣手续费；同时包含 Maker 和 Taker 的订单显示 `Maker / Taker`。
- 如果启动前或断线期间遗漏了部分成交，收到的成交量之和会与币安累计成交量不符。汇总会标注“成交记录不完整”，并将手续费、盈亏和笔数标为已接收成交的统计；合约成交额也只显示已接收部分，不将缺失记录当作零。若错过订单结束事件，该订单会继续留在本地等待，不会自动通过 REST 查询补齐。

接口参考：[币安现货签名订阅](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/user-data-stream)、[现货成交事件](https://developers.binance.com/en/docs/products/spot/user-data-stream)、[U 本位合约用户数据流](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams)、[合约 listenKey 管理](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/user-data-streams)、[Telegram sendMessage](https://core.telegram.org/bots/api#sendmessage)。

## 发布新版 Package

GitHub Actions 在每次 main 提交及 PR 上执行竞态测试、静态检查和编译。推送 `v*` 版本标签会在测试通过后构建两个架构的镜像并发布到 GHCR，使用 Actions 自带的 `GITHUB_TOKEN`，无需存入币安或 Telegram 密钥。镜像包含源码仓库与提交版本的 OCI 元数据。

```sh
git tag -a v0.1.3 -m "Release v0.1.3"
git push origin v0.1.3
gh run list --workflow ci-publish.yaml
```

首次发布的 GHCR Package 默认是 private，仓库 public 不会自动改变包的可见性。首次发布后，在 GitHub 的 Package settings → Change visibility 中设为 public；后续同一包的版本会沿用这个设置。
