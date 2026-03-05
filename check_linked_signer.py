#!/usr/bin/env python3
"""
验证 Nado 子账号的 linked signer 配置是否生效。

用法：
  1. 确保 .env 已配置：NADO_OWNER_ADDRESS、NADO_SUBACCOUNT_NAME、NADO_PRIVATE_KEY、NADO_ENV
  2. 运行: python check_linked_signer.py

若「当前子账号 linked signer」与「NADO_PRIVATE_KEY 对应地址」一致，说明可以用该私钥下单。
参考: https://docs.nado.xyz/developer-resources/api/gateway/queries/linked-signer
"""
import asyncio
import os
import sys

from dotenv import load_dotenv

load_dotenv()


def main():
    try:
        from eth_account import Account
    except ImportError:
        print("请先安装: pip install eth-account")
        sys.exit(1)

    owner = os.getenv("NADO_OWNER_ADDRESS", "").strip()
    sub_name = os.getenv("NADO_SUBACCOUNT_NAME", "default").strip()
    pk = os.getenv("NADO_PRIVATE_KEY", "").strip()

    if not owner:
        print("请在 .env 中设置 NADO_OWNER_ADDRESS（Ledger/主钱包地址）")
        sys.exit(1)
    if not pk:
        print("请在 .env 中设置 NADO_PRIVATE_KEY（用于签名的私钥）")
        sys.exit(1)

    # 当前 .env 里私钥对应的地址（你期望的 linked signer）
    try:
        acc = Account.from_key(pk)
        signing_address = acc.address.lower()
    except Exception as e:
        print(f"NADO_PRIVATE_KEY 无效: {e}")
        sys.exit(1)

    async def run():
        from exchanges.nado_adapter import NadoAdapter

        adapter = NadoAdapter(market_id=0, subaccount_name=sub_name or "default")
        sender_bytes32 = adapter._get_sender_bytes32()
        linked = await adapter.get_linked_signer()
        await adapter.close()

        env_label = os.getenv("NADO_ENV", "testnet").lower()
        gateway = "https://gateway.prod.nado.xyz/v1" if env_label == "mainnet" else "https://gateway.test.nado.xyz/v1"
        print("子账号 (subaccount bytes32，可用于 curl 查询):")
        print(f"  {sender_bytes32}")
        print(f"  手动验证: curl \"{gateway}/query?type=linked_signer&subaccount={sender_bytes32}\"")
        print()
        print("子账号: Ledger/主钱包地址 + 子账号名")
        print(f"  NADO_OWNER_ADDRESS   = {owner}")
        print(f"  NADO_SUBACCOUNT_NAME = {sub_name or 'default'}")
        print()
        print("当前子账号绑定的 linked signer (API 返回):")
        if linked:
            linked_lower = linked.lower()
            print(f"  {linked_lower}")
        else:
            print("  (无 linked signer 或查询失败)")
        print()
        print(".env 中 NADO_PRIVATE_KEY 对应的签名地址:")
        print(f"  {signing_address}")
        print()

        if linked:
            if linked_lower == signing_address:
                print("结果: 一致，可以用当前 NADO_PRIVATE_KEY 下单。")
            else:
                print("结果: 不一致，请把子账号的 linked signer 设为上面「签名地址」，或把 NADO_PRIVATE_KEY 改为当前 linked signer 的私钥。")
        else:
            print("结果: 未查到 linked signer，请先在 Nado 为该子账号绑定 linked signer（如 1CT 或 API link_signer）。")

    asyncio.run(run())


if __name__ == "__main__":
    main()
