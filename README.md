# perpdex-nado-aave

Nado 上的 AAVE 永续网格交易程序。

## 快速开始

1. 创建虚拟环境并安装依赖

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. 配置 `.env`

可参考仓库根目录 `.env.example`：

- `EXCHANGE_TYPE=nado_perp` 或 `nado_spot`（合约与现货二选一）
- `MARKET_ID=0`
- `NADO_OWNER_ADDRESS=...`（可选，子账号 owner 地址）
- `NADO_PRIVATE_KEY=...`（EIP712 签名用私钥）
- `NADO_ENV=testnet` 或 `mainnet`
- `NADO_SYMBOL=AAVEUSDT0`
- `NADO_ISOLATED=false`（可选：逐仓下单开关；若该合约是 isolated-only 且报 `error_code=2122`，设为 true）
- `NADO_ISOLATED_MARGIN_USDC=0`（可选：isolated 下单附带初始保证金，写入 appendix 高位；如 1000 表示 1000 USDC）
- `RISK_ENABLED=true`（可选：是否启用风控；设为 `false` 时跳过 Binance K线风控与相关参数依赖）
- `OPEN_ORDER_PRICE_GUARD_ENABLED=false`（可选：是否启用价格阈值暂停开仓）
- `OPEN_ORDER_PRICE_GUARD_ACTION=pause_open_orders`（可选：价格触发后只暂停开仓，或撤单并停机）
- `OPEN_ORDER_PRICE_GUARD_MIN_PRICE=`（可选：当前价 `<=` 该值时暂停开仓；留空表示不设下限）
- `OPEN_ORDER_PRICE_GUARD_MAX_PRICE=`（可选：当前价 `>=` 该值时暂停开仓；留空表示不设上限）
- `WS_REPLENISH_TIMEOUT_SEC=30`（补单保护：WS 成交补单超时秒，防止配对平仓无限重试时持锁卡死补单系统）
- `PAIRED_CLOSE_STAGE3_MAX_RETRY=30`（配对平仓 3x 阶段最大重试次数，耗尽后交由对账兜底）
- `CLOSE_ORDER_RECONCILE_ENABLED=true`（平仓单对账兜底，建议开启）
- `CLOSE_ORDER_RECONCILE_MAX_PER_ROUND=3`（对账每轮最多补几张）
- `TRAILING_MAX_POSITION_RATIO=0.3`（仓位低于 `ALER_POSITION*该值` 时允许追价；0 表示仅空仓追价）
- `GRID_TRANSLATE_ENABLED=true`（向价格方向补单后撤掉最远单，使网格整体平移）
- `OPEN_ORDER_FOLLOW_MARKET_ENABLED=false`（激进：开仓单距市价过远时直接贴近市价挂单）
- `RISK_BINANCE_SYMBOL=AAVEUSDT`
- `RISK_BINANCE_MARKET=spot`（可选：`spot` 现货 / `futures` 合约，默认 spot）
- `RISK_KLINE_COUNT=100`
- `NADO_PRODUCT_ID=`（可留空，程序会在启动时自动按 `NADO_SYMBOL` 解析）

使用方式建议：

- **直接用主钱包私钥（简单）**：只设置 `NADO_PRIVATE_KEY=主钱包私钥`，`NADO_OWNER_ADDRESS` 留空（默认等于私钥地址）。
- **使用 linked signer / 1-Click Trading（推荐）**：在 Nado 文档 [Linked Signers](https://docs.nado.xyz/developer-resources/get-started/linked-signers) 中为子账号绑定一个签名地址，然后：
  - `NADO_OWNER_ADDRESS=主钱包地址`（实际持有资金的地址）
  - `NADO_PRIVATE_KEY=linked signer / 1CT 私钥`（仅用于签名）

注意：`grid.py` 已启用严格配置模式，网格与风控参数必须在 `.env` 中完整配置，缺失会直接报错退出。

3. 启动

```bash
python grid.py
```

## 说明

- 程序支持 Nado 永续合约（`nado_perp`）与现货（`nado_spot`），通过 `EXCHANGE_TYPE` 二选一。
- 默认交易标的是 `AAVEUSDT0`（建议显式设置 `NADO_PRODUCT_ID=26`）。
- 当 `RISK_ENABLED=true` 时，风控 K 线数据源由 `RISK_BINANCE_SYMBOL` 配置决定（例如 `AAVEUSDT`），市场（现货/合约）由 `RISK_BINANCE_MARKET` 决定。
- 当 `RISK_ENABLED=false` 时，程序跳过 Binance K 线风控、ATR 动态步长和趋势过滤，可用于 Binance 无对应交易对的标的（如 WTI）。
- 当 `OPEN_ORDER_PRICE_GUARD_ENABLED=true` 且 `OPEN_ORDER_PRICE_GUARD_ACTION=pause_open_orders` 时，若当前价超出你配置的上下限，程序只暂停“开仓单”下发；平仓单、成交处理、日志和主循环继续运行。价格回到允许区间后，会自动恢复开仓。
- 当 `OPEN_ORDER_PRICE_GUARD_ENABLED=true` 且 `OPEN_ORDER_PRICE_GUARD_ACTION=cancel_orders_and_stop` 时，若当前价超出你配置的上下限，程序会先撤销当前活跃挂单，再优雅退出程序；后续需要手动重启。

## 补单保护与追价

本节对应 `WS_REPLENISH_TIMEOUT_SEC` / `CLOSE_ORDER_RECONCILE_*` / `TRAILING_*` / `GRID_TRANSLATE_ENABLED` 几个参数。

### 为什么需要平仓单对账

原有的「开仓成交必有止盈单」由三层事件驱动逻辑保证：配对平仓重试状态机、计算失败回退、REST 消失单检测。这三层都需要先有一个成交事件把链路点着，链路本身断掉时没有兜底。

`CLOSE_ORDER_RECONCILE_ENABLED=true` 会在每轮补单末尾比对「可用仓位应有的平仓单格数」与「实际挂着的平仓单数」，发现缺口就按步长在市价外侧找空位补齐（`reduce_only`，按 `0.5*step` 容差跳过已占用价位和在途配对单目标价）。它是幂等的，不关心缺口成因，因此能收敛 WS 丢帧、配对重试耗尽、下单被拒、程序重启等各种情况。

### 为什么配对平仓要限制重试次数

配对平仓原先在 3x 阶段无限重试，而它运行在持有补单锁的路径上。交易所持续拒单时该协程永远不退出，主循环的补单会一直拿不到锁，日志上只表现为「执行循环检查超时」，此后所有成交都不再处理。现在 3x 阶段重试 `PAIRED_CLOSE_STAGE3_MAX_RETRY` 次后放弃，缺口交给对账补齐；WS 路径也加了 `WS_REPLENISH_TIMEOUT_SEC` 超时作为第二道保险。

### 价格单边移动时网格如何跟随

追价（撤掉最远开仓单、重挂到市价旁）原先只在 `close_orders_count == 0` 时才允许，也就是一张止盈单都没有的近似空仓状态。只要持仓，网格就完全不跟随价格移动。

- `TRAILING_MAX_POSITION_RATIO` 放宽了这个条件：仓位低于 `ALER_POSITION * 该比例` 时也允许追价。重仓时仍然不追——那时往价格方向挂开仓单等于高位加仓，会推高持仓成本。设为 `0` 可恢复旧行为。
- `GRID_TRANSLATE_ENABLED=true` 让「向价格方向补一档」之后撤掉最远的超额开仓单（保留 `GRID_COUNT` 张），使网格整体平移。关闭时开仓单只增不减，撞到 `MAX_TOTAL_ORDERS` 后会彻底停止跟随价格。
- `OPEN_ORDER_FOLLOW_MARKET_ENABLED` 默认关闭。开启后开仓单距市价超过 `OPEN_ORDER_FOLLOW_MARKET_GAP_MULT` 倍步长时会直接贴到市价旁一档，而不是每轮只爬一档。快速上涨时可能追在相对高位，按需启用。
