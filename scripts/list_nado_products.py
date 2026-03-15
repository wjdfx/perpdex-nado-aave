#!/usr/bin/env python3
"""
列出 Nado 永续产品的 product_id 与估算现价，辅助定位某币种对应的 product_id。

注意：
- 当前 all_products 接口的 perp_products 通常不带 symbol/name，只能通过价格 + 外部行情来辅助识别；
- 可通过 --binance-symbol 指定 Binance 交易对（如 ENAUSDT），脚本会拉取该对现价并按「价格接近程度」给出候选 product_id 列表。

用法:
    python scripts/list_nado_products.py
    python scripts/list_nado_products.py --binance-symbol ENAUSDT --env mainnet
    python scripts/list_nado_products.py -b BTCUSDT -e testnet --top 10
"""
import argparse
import asyncio
import os
import sys

# 让项目根目录在 path 里
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp


def _default_env() -> str:
    raw = (os.getenv("NADO_ENV") or "mainnet").strip().lower()
    if raw in ("mainnet", "testnet"):
        return raw
    if raw in ("prod", "production"):
        return "mainnet"
    if raw in ("test", "dev"):
        return "testnet"
    return "mainnet"


def parse_args():
    parser = argparse.ArgumentParser(
        description="列出 Nado 永续产品 product_id 与估算现价，可选按 Binance 价格匹配候选。"
    )
    parser.add_argument(
        "-e", "--env",
        choices=["mainnet", "testnet"],
        default=None,
        help="Nado 环境：mainnet 或 testnet（也可通过环境变量 NADO_ENV：mainnet/testnet/prod/test，默认 mainnet）",
    )
    parser.add_argument(
        "-b", "--binance-symbol",
        type=str,
        default=None,
        metavar="SYMBOL",
        help="Binance 交易对，如 ENAUSDT、BTCUSDT；指定后会拉取该对现价并按价格接近程度输出候选 product_id",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        metavar="N",
        help="当指定 --binance-symbol 时，输出与参考价最接近的前 N 个候选（默认 5）",
    )
    return parser.parse_args()


async def fetch_binance_price(symbol: str) -> float:
    """从 Binance 获取指定交易对的现价。"""
    url = "https://api.binance.com/api/v3/ticker/price"
    params = {"symbol": symbol.upper()}
    connector = aiohttp.TCPConnector(ssl=False)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status != 200:
                    return 0.0
                data = await resp.json()
                price = float(data.get("price") or 0.0)
                return price
    except Exception:
        return 0.0


async def main():
    args = parse_args()
    env = args.env if args.env is not None else _default_env()
    if env == "mainnet":
        base = "https://gateway.prod.nado.xyz/v1"
    else:
        base = "https://gateway.test.nado.xyz/v1"
    url = f"{base}/query"
    params = {"type": "all_products"}
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                print(f"请求失败: {resp.status} {await resp.text()}")
                return
            data = await resp.json()
    if data.get("status") != "success":
        print("API 返回失败:", data.get("error", data))
        return
    payload = data.get("data", {})
    perp = payload.get("perp_products", [])
    if not perp:
        print("未获取到 perp_products，请检查 API 或环境。")
        return

    ref_price = 0.0
    if args.binance_symbol:
        ref_price = await fetch_binance_price(args.binance_symbol)

    rows = []
    for p in perp:
        pid = p.get("product_id")
        price_x18_str = (
            p.get("risk", {}).get("price_x18")
            or p.get("oracle_price_x18")
            or "0"
        )
        try:
            price_x18 = int(price_x18_str)
        except Exception:
            price_x18 = 0
        price = price_x18 / 1e18 if price_x18 else 0.0
        diff = abs(price - ref_price) if ref_price > 0 and price > 0 else None
        rows.append((pid, price, price_x18, diff))

    diff_col = f"  |price - {args.binance_symbol or 'ref'}|" if args.binance_symbol else "  (无参考价)"
    print(f"Nado {env} 永续产品 (共 {len(perp)} 个):")
    print("-" * 80)
    header = f"  product_id |    approx_price  |  raw_price_x18                  |  {diff_col}"
    print(header)
    print("-" * 80)
    for pid, price, price_x18, diff in sorted(rows, key=lambda r: r[0] or 0):
        diff_str = f"{diff:.6f}" if diff is not None else "-"
        print(f"  {pid:>10} | {price:>14.6f} | {price_x18:<28} |  {diff_str:>10}")
    print("-" * 80)

    if ref_price > 0 and args.binance_symbol:
        print(f"参考: 当前 Binance {args.binance_symbol} 价格 ≈ {ref_price:.6f}")
        top_n = max(1, min(args.top, len(rows)))
        top_candidates = sorted(
            [r for r in rows if r[3] is not None],
            key=lambda r: r[3]
        )[:top_n]
        print(f"按与 {args.binance_symbol} 现价最接近排序的前 {len(top_candidates)} 个 perp 产品：")
        for pid, price, _, diff in top_candidates:
            print(f"  candidate product_id={pid}, approx_price={price:.6f}, |diff|={diff:.6f}")
        print("请在 Nado 前端中切换到上述 candidate 对应的合约，对比价格/盘口确认。")
    elif args.binance_symbol:
        print(f"提示：未能获取 Binance {args.binance_symbol} 价格，只能根据 approx_price 手动在前端对照现价来判断。")
    else:
        print("提示：未指定 --binance-symbol，仅列出价格。可用 -b ENAUSDT 等按参考价匹配候选 product_id。")


if __name__ == "__main__":
    asyncio.run(main())
