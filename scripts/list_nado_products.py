#!/usr/bin/env python3
"""
列出 Nado 永续产品 product_id 与估算现价，用于确认 ETH / ENA 等品种的 product_id。
注意：当前 Nado all_products 接口不返回 symbol/name，只能通过价格来识别品种。
用法: python scripts/list_nado_products.py
"""
import asyncio
import os
import sys

# 让项目根目录在 path 里
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp


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
    print(f"Nado {env} 永续产品 (共 {len(perp)} 个):")
    print("-" * 70)
    print("  product_id |    approx_price  |  raw_price_x18")
    print("-" * 70)
    for p in sorted(perp, key=lambda x: x.get("product_id") or 0):
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
        print(f"  {pid:>10} | {price:>14.4f} | {price_x18}")
    print("-" * 70)
    print("提示：上表目前无法直接显示 symbol，请根据大致价格对照官方 UI，")
    print("例如：BTC ~7万、ETH ~2千、AAVE ~百多、ENA ~当前现价，对应那一行的 product_id 即为 NADO_PRODUCT_ID。")


if __name__ == "__main__":
    asyncio.run(main())
