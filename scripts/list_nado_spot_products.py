#!/usr/bin/env python3
"""
列出 Nado 现货产品（spot_products），辅助定位 spot product_id。

用法:
    python scripts/list_nado_spot_products.py
    python scripts/list_nado_spot_products.py --env mainnet
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp


def _default_env() -> str:
    raw = (os.getenv("NADO_ENV") or "mainnet").strip().lower()
    if raw in ("mainnet", "testnet"):
        return raw
    if raw in ("prod", "production"):
        return "mainnet"
    return "mainnet"


async def main():
    parser = argparse.ArgumentParser(description="列出 Nado 现货产品 spot_products")
    parser.add_argument("-e", "--env", choices=["mainnet", "testnet"], default=None)
    args = parser.parse_args()
    env = args.env or _default_env()

    base = "https://gateway.prod.nado.xyz/v1" if env == "mainnet" else "https://gateway.test.nado.xyz/v1"
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
    spot = payload.get("spot_products", [])
    perp = payload.get("perp_products", [])

    if spot:
        print(f"Nado {env} 现货产品 (共 {len(spot)} 个):")
        print("-" * 60)
        for p in sorted(spot, key=lambda x: x.get("product_id", 0)):
            pid = p.get("product_id")
            sym = p.get("symbol", p.get("instrument", p.get("name", f"PRODUCT_{pid}")))
            risk = p.get("risk", {})
            price_x18 = risk.get("price_x18") or p.get("oracle_price_x18", "0")
            try:
                price = int(price_x18) / 1e18 if price_x18 else 0
            except Exception:
                price = 0
            print(f"  product_id={pid:<6} | symbol={sym:<20} | price≈{price:.4f}")
        print("-" * 60)
    else:
        print(f"Nado {env} 未返回 spot_products。")
        print("若 Nado 尚未支持 spot，请参考文档或联系 Nado 团队。")
        print("当前 all_products 返回的 perp_products 数量:", len(perp))


if __name__ == "__main__":
    asyncio.run(main())
