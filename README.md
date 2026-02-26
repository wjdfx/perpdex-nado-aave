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

可参考仓库根目录 `example`：

- `EXCHANGE_TYPE=nado`
- `MARKET_ID=0`
- `NADO_PRIVATE_KEY=...`
- `NADO_ENV=testnet` 或 `mainnet`
- `NADO_SYMBOL=AAVE-PERP`
- `NADO_PRODUCT_ID=`（可留空，程序会在启动时自动按 `NADO_SYMBOL` 解析）

3. 启动

```bash
python grid.py
```

## 说明

- 程序仅支持 `nado`。
- 默认交易标的是 `AAVE-PERP`。
- 风控 K 线数据源默认使用 `AAVEUSDT`。
