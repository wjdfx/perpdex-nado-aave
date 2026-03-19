# Scripts 说明

本目录包含辅助运维与排查的脚本。

## list_nado_products.py — 查询 Nado 永续 product_id

列出 Nado 永续合约的 `product_id` 与估算现价，用于在无 symbol 名称时通过价格匹配确认某币种对应的 product_id（如配置网格策略时的 `MARKET_ID` / product_id）。

### 依赖

- Python 3.7+
- `aiohttp`（项目依赖中已包含）

### 用法

```bash
# 仅列出 Nado 永续产品及估算价格（不拉参考价）
python scripts/list_nado_products.py

# 指定 Binance 交易对，按价格接近程度输出候选 product_id（常用）
python scripts/list_nado_products.py --binance-symbol ENAUSDT

# 简写
python scripts/list_nado_products.py -b ENAUSDT

# 指定 Nado 环境（默认 mainnet，也可用环境变量 NADO_ENV）
python scripts/list_nado_products.py -b BTCUSDT --env mainnet
python scripts/list_nado_products.py -b ENAUSDT -e testnet

# 输出与参考价最接近的前 10 个候选
python scripts/list_nado_products.py -b ENAUSDT --top 10
```

### 参数说明

| 参数 | 简写 | 说明 | 默认 |
|------|------|------|------|
| `--env` | `-e` | Nado 环境：`mainnet` 或 `testnet` | 环境变量 `NADO_ENV`，否则 `mainnet` |
| `--binance-symbol` | `-b` | Binance 交易对（如 ENAUSDT、BTCUSDT），用于拉取参考价并按价格接近程度排序候选 | 不指定则仅列产品与价格 |
| `--top` | — | 指定 `-b` 时，输出与参考价最接近的前 N 个 product_id | 5 |

### 使用说明

- 未指定 `-b` 时：只打印所有永续产品的 `product_id` 与 `approx_price`，需自行对照交易所前端判断对应币种。
- 指定 `-b SYMBOL` 时：会请求 Binance 该交易对现价，并计算每个 Nado 永续价与参考价的差值，按差值排序输出前 `--top` 个候选，便于快速定位（如 ENA 对应哪个 product_id）。最终仍需在 Nado 前端切换合约对比盘口确认。

补充：
- 脚本会**先查 Binance 现货**价格；若该交易对不在现货（常见：只在合约端有），会自动回退查 **Binance USDT 合约（fapi）**。
- 如果两边都失败，会打印 `[WARN]` 提示（HTTP 状态码与返回体片段），便于排查是交易对不存在、地区/网络限制、或被限流等原因。
