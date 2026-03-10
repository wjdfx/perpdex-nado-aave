#!/usr/bin/env python3
"""
列出 Nado 永续产品 product_id 与 symbol，用于确认 ETH 等品种的 product_id。
无需私钥，直接请求 Nado 公开 API。
用法: python scripts/list_nado_products.py
"""
import asyncio
import os
import sys

# 让项目根目录在 path 里
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp


def extract_symbol(p: dict) -> str:
    for key in ("symbol", "instrument", "name", "product_name", "display_name"):
        v = p.get(key)
        if isinstance(v, str) and v:
            return v
    return ""


async def main():
    env = os.getenv("NADO_ENV", "mainnet").lower()
    if env == "mainnet":
        base = "https://gateway.prod.nado.xyz/v1"
    else:
        base = "https://gateway.test.nado.xyz/v1"
    url = f"{base}/query"
    params = {"type": "all_products"}
    async with aiohttp.ClientSession() as session:
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
    print("-" * 50)
    for p in sorted(perp, key=lambda x: x.get("product_id") or 0):
        pid = p.get("product_id")
        sym = extract_symbol(p)
        print(f"  product_id={pid}  ->  {sym or '(无 symbol)'}")
    print("-" * 50)
    print("请根据上面列表设置 NADO_PRODUCT_ID（例如 ETH 对应那一行的 product_id）。")


if __name__ == "__main__":
    asyncio.run(main())
