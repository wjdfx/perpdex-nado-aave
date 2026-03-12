#!/usr/bin/env python3
"""
列出 Nado 永续产品的 product_id 与估算现价，辅助定位 ENA 等币种对应的 product_id。

注意：
- 当前 all_products 接口的 perp_products 通常不带 symbol/name，只能通过价格 + 外部行情来辅助识别；
- 本脚本额外从 Binance 拉 ENAUSDT 价格，并按「价格接近程度」给出 ENA 候选 product_id 列表。

用法:
    python scripts/list_nado_products.py
"""
import asyncio
import os
import sys

# 让项目根目录在 path 里
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp


async def fetch_ena_price_binance() -> float:
    """从 Binance 获取 ENAUSDT 的现价，用于辅助定位 ENA 的 product_id。"""
    url = "https://api.binance.com/api/v3/ticker/price"
    params = {"symbol": "ENAUSDT"}
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
    env = os.getenv("NADO_ENV", "mainnet").lower()
    if env == "mainnet":
        base = "https://gateway.prod.nado.xyz/v1"
    else:
        base = "https://gateway.test.nado.xyz/v1"
    url = f"{base}/query"
    params = {"type": "all_products"}
    # 某些环境本地根证书不完整，这里关闭 SSL 校验仅用于查询公开产品列表
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
    # 尝试从 Binance 获取 ENAUSDT 现价
    ena_ref = await fetch_ena_price_binance()

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
        diff = abs(price - ena_ref) if ena_ref > 0 and price > 0 else None
        rows.append((pid, price, price_x18, diff))

    print(f"Nado {env} 永续产品 (共 {len(perp)} 个):")
    print("-" * 80)
    header = "  product_id |    approx_price  |  raw_price_x18                  |  |price - ENA|"
    print(header)
    print("-" * 80)
    for pid, price, price_x18, diff in sorted(rows, key=lambda r: r[0] or 0):
        diff_str = f"{diff:.6f}" if diff is not None else "-"
        print(f"  {pid:>10} | {price:>14.6f} | {price_x18:<28} |  {diff_str:>10}")
    print("-" * 80)
    if ena_ref > 0:
        print(f"参考: 当前 Binance ENAUSDT 价格 ≈ {ena_ref:.6f}")
        top_candidates = sorted(
            [r for r in rows if r[3] is not None],
            key=lambda r: r[3]
        )[:5]
        print("按与 ENAUSDT 现价最接近排序的前 5 个 perp 产品：")
        for pid, price, _, diff in top_candidates:
            print(f"  candidate product_id={pid}, approx_price={price:.6f}, |diff|={diff:.6f}")
        print("请在 Nado 前端中切换到上述 candidate 对应的合约，对比价格/盘口确认哪一个是 ENA。")
    else:
        print("提示：未能获取 Binance ENAUSDT 价格，只能根据 approx_price 手动在前端对照 ENA 的现价来判断。")


if __name__ == "__main__":
    asyncio.run(main())
