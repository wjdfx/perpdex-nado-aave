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

- `EXCHANGE_TYPE=nado`
- `MARKET_ID=0`
- `NADO_OWNER_ADDRESS=...`（可选，子账号 owner 地址）
- `NADO_PRIVATE_KEY=...`（EIP712 签名用私钥）
- `NADO_ENV=testnet` 或 `mainnet`
- `NADO_SYMBOL=AAVEUSDT0`
- `NADO_ISOLATED=false`（可选：逐仓下单开关；若该合约是 isolated-only 且报 `error_code=2122`，设为 true）
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

- 程序仅支持 `nado`。
- 默认交易标的是 `AAVEUSDT0`（建议显式设置 `NADO_PRODUCT_ID=26`）。
- 风控 K 线数据源由 `RISK_BINANCE_SYMBOL` 配置决定（例如 `AAVEUSDT`），市场（现货/合约）由 `RISK_BINANCE_MARKET` 决定。
