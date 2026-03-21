"""
Nado Spot Exchange Adapter

独立 Spot 适配器，实现 ExchangeInterface 用于 Nado.xyz 现货交易（如 wETH/USDC）。
复用永续适配器的 EIP712 签名与网关调用逻辑，但适配 spot 语义：
- 无 reduce_only（现货无持仓概念，所有订单为普通买卖）
- 仓位由 base 余额表示
- 不涉及 isolated margin

注意：不修改 nado_adapter.py，保持永续合约功能完整。
"""

import asyncio
import contextlib
import json
import logging
import os
import time
import random
import re
from typing import List, Tuple, Dict, Callable, Any, Optional
from decimal import Decimal

import aiohttp
import pandas as pd
from eth_account import Account
from eth_account.messages import encode_typed_data

from .interfaces import ExchangeInterface
from .order_converter import normalize_order_to_ccxt, normalize_orders_list

logger = logging.getLogger(__name__)

X18 = 10 ** 18


class NadoSpotAdapter(ExchangeInterface):
    """
    Nado Spot 现货适配器。
    将 base 余额映射为「仓位」，所有订单为普通 limit 买卖（无 reduce_only）。
    """

    TARGET_SYMBOL = os.getenv("NADO_SPOT_SYMBOL", os.getenv("NADO_SYMBOL", "WETHUSDC"))

    # Spot 产品 ID 映射（与 perp 分开，可扩展）
    # PRODUCT_ID_TO_SYMBOL 为兼容 quant_grid 日志
    SPOT_PRODUCT_ID_TO_SYMBOL = PRODUCT_ID_TO_SYMBOL = {
        0: "USDC",
        1: "WETH",
        # 按 Nado 实际 spot_products 扩展
    }

    @staticmethod
    def _env_int(name: str, default: int, min_value: int = 0) -> int:
        raw = os.getenv(name, "").strip()
        if not raw:
            return default
        try:
            value = int(raw)
            if value < min_value:
                return default
            return value
        except Exception:
            return default

    def __init__(
        self,
        market_id: int = 0,
        product_id: int = None,
        subaccount_name: str = "default"
    ):
        self.market_id = market_id
        self.target_symbol = self.TARGET_SYMBOL
        env_product_id = os.getenv("NADO_SPOT_PRODUCT_ID", os.getenv("NADO_PRODUCT_ID", "")).strip()
        env_product_id_int = int(env_product_id) if env_product_id.isdigit() else None
        self.product_id = product_id if product_id is not None else env_product_id_int
        self.subaccount_name = subaccount_name

        self.private_key = os.getenv('NADO_PRIVATE_KEY', '')
        self.env = os.getenv('NADO_ENV', 'testnet').lower()

        if self.env == 'mainnet':
            self.gateway_rest = os.getenv('NADO_GATEWAY_REST', 'https://gateway.prod.nado.xyz/v1')
            self.subscriptions_ws = os.getenv('NADO_SUBSCRIPTIONS_WS', 'wss://gateway.prod.nado.xyz/v1/subscribe')
        else:
            self.gateway_rest = os.getenv('NADO_GATEWAY_REST', 'https://gateway.test.nado.xyz/v1')
            self.subscriptions_ws = os.getenv('NADO_SUBSCRIPTIONS_WS', 'wss://gateway.test.nado.xyz/v1/subscribe')

        self.recv_time_offset_ms = self._env_int("NADO_RECV_TIME_OFFSET_MS", 1200, min_value=1)

        if self.private_key:
            self.account = Account.from_key(self.private_key)
            self.address = self.account.address
        else:
            self.account = None
            self.address = None

        owner_address_env = os.getenv("NADO_OWNER_ADDRESS", "").strip()
        if owner_address_env:
            normalized = owner_address_env if owner_address_env.startswith("0x") else "0x" + owner_address_env
            self.owner_address = normalized.lower()
        else:
            self.owner_address = self.address.lower() if self.address else None

        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_session: Optional[aiohttp.ClientSession] = None
        self.subscriptions_ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self.callbacks: Dict[str, Callable] = {}
        self.ws_initialized = False
        self._closing = False
        self._rest_polling_task_handle: Optional[asyncio.Task] = None
        self._ws_listener_task_handle: Optional[asyncio.Task] = None
        self._ws_ping_task_handle: Optional[asyncio.Task] = None

        self.chain_id: Optional[int] = None
        self.endpoint_address: Optional[str] = None
        self.order_digests: Dict[str, str] = {}

        self.price_increment: float = 0.01
        self.size_increment: float = 0.001
        self.min_size: float = 10.0

        logger.info(
            f"Nado Spot Adapter initialized: env={self.env}, symbol={self.target_symbol}, product_id={self.product_id}"
        )

    def _get_sender_bytes32(self) -> str:
        if not self.owner_address:
            raise ValueError("NADO_OWNER_ADDRESS or NADO_PRIVATE_KEY not set")
        subaccount_bytes = self.subaccount_name.encode('utf-8')[:12].ljust(12, b'\x00')
        return f"0x{self.owner_address[2:]}{subaccount_bytes.hex()}"

    def _gen_order_nonce(self) -> int:
        timestamp_ms = int(time.time() * 1000)
        recv_time = timestamp_ms + self.recv_time_offset_ms
        return (recv_time << 20) + random.randint(0, (1 << 20) - 1)

    def _gen_order_verifying_contract(self, product_id: int) -> str:
        be_bytes = product_id.to_bytes(20, byteorder="big", signed=False)
        return "0x" + be_bytes.hex()

    def _to_x18(self, value: float) -> int:
        return int(Decimal(str(value)) * X18)

    def _from_x18(self, value: int) -> float:
        return float(Decimal(str(value)) / X18)

    def _round_price_x18(self, price_x18: int) -> int:
        inc_x18 = self._to_x18(self.price_increment)
        return (price_x18 // inc_x18) * inc_x18 if inc_x18 > 0 else price_x18

    def _round_size_x18(self, size_x18: int) -> int:
        inc_x18 = self._to_x18(self.size_increment)
        return (size_x18 // inc_x18) * inc_x18 if inc_x18 > 0 else size_x18

    def _build_appendix_spot(
        self,
        order_type: int = 3,
        reduce_only: bool = False
    ) -> int:
        """
        Spot 订单 appendix（Nado 现货也支持 reduce_only）。
        bit 11: reduce_only
        """
        appendix = 1 | (order_type << 9)
        if reduce_only:
            appendix |= (1 << 11)
        return appendix

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=12, connect=5, sock_read=10),
                trust_env=True,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'User-Agent': 'perpdex-nado-aave/1.0-spot (+aiohttp)'
                }
            )
        return self.session

    def _sign_typed_data(
        self,
        primary_type: str,
        types: Dict,
        message: Dict,
        verifying_contract: str
    ) -> str:
        if not self.account:
            raise ValueError("Private key not set")
        domain = {
            "name": "Nado",
            "version": "0.0.1",
            "chainId": self.chain_id,
            "verifyingContract": verifying_contract
        }
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"}
                ],
                **types
            },
            "primaryType": primary_type,
            "domain": domain,
            "message": message
        }
        signable = encode_typed_data(full_message=typed_data)
        return self.account.sign_message(signable).signature.hex()

    async def _rest_query(self, query_type: str, params: Dict = None) -> Dict:
        if self._closing:
            return {}
        query_params = {"type": query_type}
        if params:
            query_params.update(params)
        url = f"{self.gateway_rest}/query"
        for attempt in range(1, 4):
            try:
                session = await self._get_session()
                async with session.get(url, params=query_params) as response:
                    if response.status != 200:
                        if attempt < 3:
                            await asyncio.sleep(0.4 * attempt)
                            continue
                        return {}
                    data = await response.json(content_type=None)
                    if data.get('status') == 'success':
                        return data.get('data', {})
                    return {}
            except Exception as e:
                if attempt < 3:
                    await asyncio.sleep(0.4 * attempt)
                    continue
                logger.error(f"REST query {query_type} failed: {e}")
                return {}
        return {}

    async def _rest_execute(self, payload: Dict) -> Dict:
        if self._closing:
            return {"status": "failure", "error": "adapter closing"}
        url = f"{self.gateway_rest}/execute"
        for attempt in range(1, 4):
            try:
                session = await self._get_session()
                async with session.post(url, json=payload) as response:
                    if response.status != 200:
                        text = await response.text()
                        if attempt < 3 and response.status >= 500:
                            await asyncio.sleep(0.4 * attempt)
                            continue
                        return {"status": "failure", "error": f"http {response.status}: {text[:200]}"}
                    return await response.json(content_type=None)
            except Exception as e:
                if attempt < 3:
                    await asyncio.sleep(0.4 * attempt)
                    continue
                return {"status": "failure", "error": str(e)}
        return {"status": "failure", "error": "unknown"}

    async def place_single_order(
        self,
        is_ask: bool,
        price: float,
        amount: float,
        reduce_only: bool = False
    ) -> Tuple[bool, str, str, object]:
        """
        Spot 下单。Nado 现货支持 reduce_only，会写入 appendix。
        """
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()
            expiration = int(time.time()) + 86400 * 30

            price_x18 = self._round_price_x18(self._to_x18(price))
            amount_x18 = self._round_size_x18(self._to_x18(amount))
            if is_ask:
                amount_x18 = -amount_x18

            appendix = self._build_appendix_spot(order_type=3, reduce_only=reduce_only)

            order_message = {
                "sender": sender,
                "priceX18": str(price_x18),
                "amount": str(amount_x18),
                "expiration": str(expiration),
                "nonce": str(nonce),
                "appendix": str(appendix)
            }

            order_types = {
                "Order": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "priceX18", "type": "int128"},
                    {"name": "amount", "type": "int128"},
                    {"name": "expiration", "type": "uint64"},
                    {"name": "nonce", "type": "uint64"},
                    {"name": "appendix", "type": "uint128"}
                ]
            }

            verifying_contract = self._gen_order_verifying_contract(self.product_id)
            signature = self._sign_typed_data(
                "Order", order_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "priceX18": price_x18,
                    "amount": amount_x18,
                    "expiration": expiration,
                    "nonce": nonce,
                    "appendix": appendix
                },
                verifying_contract
            )

            payload = {
                "place_order": {
                    "product_id": self.product_id,
                    "order": order_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }

            response = await self._rest_execute(payload)
            if response.get('status') == 'success':
                digest = response.get('data', {}).get('digest', '')
                client_order_id = str(nonce & ((1 << 20) - 1))
                self.order_digests[client_order_id] = digest
                return True, client_order_id, "", None
            err = response.get('error', '未知错误')
            err_code = response.get('error_code')
            return False, '', str(err), err_code
        except Exception as e:
            logger.error(f"place_single_order 错误: {e}", exc_info=True)
            return False, '', str(e), None

    async def place_multi_orders(
        self,
        orders: List[Tuple[bool, float, float]]
    ) -> Tuple[bool, List[str]]:
        if not orders:
            return True, []
        order_ids = []
        for i, (is_ask, price, amount) in enumerate(orders):
            success, oid, _, _ = await self.place_single_order(is_ask, price, amount)
            if not success:
                if order_ids:
                    await self.cancel_grid_orders(order_ids)
                return False, []
            order_ids.append(oid)
        return True, order_ids

    async def place_single_market_order(
        self,
        is_ask: bool,
        price: float,
        amount: float
    ) -> Tuple[bool, str]:
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()
            expiration = int(time.time()) + 60
            market_price = price * 1.1 if not is_ask else price * 0.9
            price_x18 = self._round_price_x18(self._to_x18(market_price))
            amount_x18 = self._round_size_x18(self._to_x18(amount))
            if is_ask:
                amount_x18 = -amount_x18
            appendix = self._build_appendix_spot(order_type=1, reduce_only=False)

            order_message = {
                "sender": sender, "priceX18": str(price_x18), "amount": str(amount_x18),
                "expiration": str(expiration), "nonce": str(nonce), "appendix": str(appendix)
            }
            order_types = {
                "Order": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "priceX18", "type": "int128"},
                    {"name": "amount", "type": "int128"},
                    {"name": "expiration", "type": "uint64"},
                    {"name": "nonce", "type": "uint64"},
                    {"name": "appendix", "type": "uint128"}
                ]
            }
            verifying_contract = self._gen_order_verifying_contract(self.product_id)
            signature = self._sign_typed_data(
                "Order", order_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "priceX18": price_x18, "amount": amount_x18,
                    "expiration": expiration, "nonce": nonce, "appendix": appendix
                },
                verifying_contract
            )
            payload = {
                "place_order": {
                    "product_id": self.product_id,
                    "order": order_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }
            response = await self._rest_execute(payload)
            if response.get('status') == 'success':
                digest = response.get('data', {}).get('digest', '')
                client_order_id = str(nonce & ((1 << 20) - 1))
                self.order_digests[client_order_id] = digest
                return True, client_order_id
            return False, ''
        except Exception as e:
            logger.error(f"place_single_market_order 错误: {e}", exc_info=True)
            return False, ''

    async def cancel_grid_orders(self, order_ids: List[str]) -> bool:
        if not order_ids:
            return True
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()
            resolved = []
            unresolved = []
            for oid in order_ids:
                digest = self.order_digests.get(oid)
                if digest:
                    resolved.append((oid, digest))
                elif isinstance(oid, str) and oid.startswith("0x") and len(oid) == 66:
                    resolved.append((oid, oid))
                else:
                    unresolved.append(oid)
            if unresolved:
                await self._rebuild_order_digest_mapping()
                for oid in unresolved:
                    digest = self.order_digests.get(oid)
                    if digest:
                        resolved.append((oid, digest))
            if not resolved:
                logger.warning("cancel_grid_orders: 无法解析任何订单 digest，跳过取消")
                return False
            digests = list({r[1] for r in resolved})
            cancel_message = {
                "sender": sender,
                "productIds": [self.product_id] * len(digests),
                "digests": digests,
                "nonce": str(nonce)
            }
            cancel_types = {
                "Cancellation": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "productIds", "type": "uint32[]"},
                    {"name": "digests", "type": "bytes32[]"},
                    {"name": "nonce", "type": "uint64"}
                ]
            }
            signature = self._sign_typed_data(
                "Cancellation", cancel_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "productIds": [self.product_id] * len(digests),
                    "digests": [bytes.fromhex(d[2:]) if d.startswith('0x') else bytes.fromhex(d) for d in digests],
                    "nonce": nonce
                },
                self.endpoint_address
            )
            payload = {
                "cancel_orders": {
                    "tx": cancel_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }
            response = await self._rest_execute(payload)
            if response.get('status') == 'success':
                for oid, _ in resolved:
                    self.order_digests.pop(oid, None)
                return True
            return False
        except Exception as e:
            logger.error(f"cancel_grid_orders 错误: {e}", exc_info=True)
            return False

    async def modify_grid_order(
        self,
        order_id: str,
        new_price: float,
        new_amount: float
    ) -> bool:
        logger.warning("Nado Spot 不支持修改订单，请先取消再创建")
        return False

    async def _rebuild_order_digest_mapping(self) -> int:
        try:
            orders = await self.get_orders()
            restored = 0
            for o in orders:
                digest = str(o.get("digest", "") or "").strip()
                cid = str(int(o.get("nonce", 0)) & ((1 << 20) - 1))
                if digest and cid and cid not in self.order_digests:
                    self.order_digests[cid] = digest
                    restored += 1
            if restored:
                logger.info("从活跃订单重建 %s 个 order digest 映射", restored)
            return restored
        except Exception as e:
            logger.error("_rebuild_order_digest_mapping 错误: %s", e, exc_info=True)
            return 0

    async def get_orders(self) -> List[dict]:
        try:
            sender = self._get_sender_bytes32()
            data = await self._rest_query("subaccount_orders", {"sender": sender, "product_id": self.product_id})
            return data.get('orders', [])
        except Exception as e:
            logger.error(f"get_orders 错误: {e}", exc_info=True)
            return []

    async def get_trades(self, limit: int = 1) -> List[dict]:
        return []

    async def _query_spot_balances(self) -> Dict[str, dict]:
        """
        查询 Spot 余额，映射为网格所需的「仓位」格式。
        Nado 若提供 spot_balances，则使用；否则尝试 perp_balances 结构或返回空。
        """
        try:
            sender = self._get_sender_bytes32()
            data = await self._rest_query("subaccount_info", {"subaccount": sender})
            spot_balances = data.get('spot_balances', [])
            spot_products = data.get('spot_products', data.get('perp_products', []))
            if not spot_balances and data.get('perp_balances'):
                spot_balances = data.get('perp_balances', [])
            positions = {}
            for balance in spot_balances:
                product_id = balance.get('product_id')
                if product_id != self.product_id:
                    continue
                bal = balance.get('balance', {})
                amount = self._from_x18(int(bal.get('amount', '0')))
                product_info = next((p for p in spot_products if p.get('product_id') == product_id), {})
                oracle_price = self._from_x18(int(product_info.get('oracle_price_x18', product_info.get('risk', {}).get('price_x18', '0'))))
                if oracle_price <= 0:
                    prices = await self._rest_query("market_price", {"product_id": product_id})
                    if prices:
                        bid = self._from_x18(int(prices.get('bid_x18', '0') or 0))
                        ask = self._from_x18(int(prices.get('ask_x18', '0') or 0))
                        oracle_price = (bid + ask) / 2 if (bid and ask) else bid or ask
                symbol = self.PRODUCT_ID_TO_SYMBOL.get(product_id, self.target_symbol)
                positions[symbol] = {
                    'instrument': symbol,
                    'product_id': product_id,
                    'position': amount,
                    'size': amount,
                    'amount': amount,
                    'sign': 1 if amount >= 0 else -1,
                    'notional': abs(amount * oracle_price) if oracle_price else 0,
                    'mark_price': oracle_price,
                }
            return positions
        except Exception as e:
            logger.error(f"查询 spot 余额失败: {e}", exc_info=True)
            return {}

    async def get_positions(self) -> Dict[str, dict]:
        positions = await self._query_spot_balances()
        if not positions and self.product_id is not None:
            symbol = self.PRODUCT_ID_TO_SYMBOL.get(self.product_id, self.target_symbol)
            positions[symbol] = {
                'instrument': symbol,
                'product_id': self.product_id,
                'position': 0,
                'size': 0,
                'amount': 0,
                'sign': 0,
                'notional': 0,
                'mark_price': 0,
            }
        return positions

    async def get_account(self) -> dict:
        try:
            sender = self._get_sender_bytes32()
            data = await self._rest_query("subaccount_info", {"subaccount": sender})
            healths = data.get('healths', [{}])
            unweighted = healths[2] if len(healths) > 2 else {}
            total_equity = self._from_x18(int(unweighted.get('assets', '0')))
            total_liability = self._from_x18(int(unweighted.get('liabilities', '0')))
            return {
                'total_equity': total_equity,
                'collateral': total_equity - total_liability,
                'free_collateral': self._from_x18(int(healths[0].get('health', '0'))) if healths else 0
            }
        except Exception as e:
            logger.error(f"get_account 错误: {e}", exc_info=True)
            return {}

    async def candle_stick(
        self,
        market_id: int,
        resolution: str,
        count_back: int = 200
    ) -> pd.DataFrame:
        from exchanges.common_market_data import BinanceMarketData
        sym = os.getenv("RISK_BINANCE_SYMBOL", self.target_symbol.replace("USDC", "USDT").replace("WETH", "ETH"))
        if "ETH" in sym.upper() and "USDT" not in sym.upper():
            sym = "ETHUSDT"
        interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
        interval = interval_map.get(resolution, "1m")
        try:
            bmd = BinanceMarketData()
            df = await asyncio.to_thread(
                bmd.get_klines_df, symbol=sym, interval=interval, limit=count_back,
                market="spot", timeout=15
            )
            if not df.empty:
                df = df.rename(columns={"open_time": "time"})
                df = df[["time", "open", "high", "low", "close", "volume"]]
            return df
        except Exception as e:
            logger.error(f"candle_stick 错误: {e}")
            return pd.DataFrame()

    async def modify_order(
        self,
        order_id: int,
        new_price: float,
        new_amount: float
    ) -> bool:
        return False

    async def get_orders_by_rest(self) -> List[dict]:
        return await self.get_orders()

    async def get_trades_by_rest(self, ask_filter: int, limit: int) -> List[dict]:
        return await self.get_trades(limit)

    async def subscribe(
        self,
        callbacks: Dict[str, Callable[[str, Any], None]],
        proxy: str = None
    ) -> None:
        self.callbacks = callbacks
        if 'market_stats' in callbacks:
            price = await self._poll_market_price()
            if price:
                logger.info(f"初始市场价格: {price}")
        if not self.ws_initialized:
            await self._initialize_ws()
        if self.ws_initialized and self.subscriptions_ws_connection:
            if 'market_stats' in callbacks:
                await self._subscribe_market_stats()
            if 'orders' in callbacks:
                await self._subscribe_orders()
            if 'positions' in callbacks:
                await self._subscribe_positions()
        if self._rest_polling_task_handle is None or self._rest_polling_task_handle.done():
            self._rest_polling_task_handle = asyncio.create_task(self._rest_polling_task())

    async def _rest_polling_task(self):
        while not self._closing:
            try:
                await self._poll_market_price()
                await self._poll_orders()
                await self._poll_positions()
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._closing:
                    logger.error(f"REST 轮询错误: {e}")
                await asyncio.sleep(5)

    async def _poll_market_price(self):
        try:
            prices = await self._rest_query("market_price", {"product_id": self.product_id})
            if prices:
                bid = self._from_x18(int(prices.get('bid_x18', '0') or 0))
                ask = self._from_x18(int(prices.get('ask_x18', '0') or 0))
                mark = (bid + ask) / 2 if (bid and ask) else bid or ask
                if mark > 0 and 'market_stats' in self.callbacks and self.callbacks['market_stats']:
                    cb = self.callbacks['market_stats']
                    stats = {'mark_price': mark, 'best_bid': bid, 'best_ask': ask}
                    if asyncio.iscoroutinefunction(cb):
                        asyncio.create_task(cb(str(self.market_id), stats))
                    else:
                        cb(str(self.market_id), stats)
                return mark
        except Exception as e:
            logger.error(f"轮询行情错误: {e}")
        return None

    async def _poll_orders(self):
        try:
            if 'orders' not in self.callbacks or not self.callbacks['orders']:
                return
            orders = await self.get_orders()
            if orders:
                norm = normalize_orders_list(orders)
                cb = self.callbacks['orders']
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb(self.address, norm))
                else:
                    cb(self.address, norm)
        except Exception as e:
            logger.error(f"轮询订单错误: {e}")

    async def _poll_positions(self):
        try:
            if 'positions' not in self.callbacks or not self.callbacks['positions']:
                return
            positions = await self.get_positions()
            if positions:
                cb = self.callbacks['positions']
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb(self.address, positions))
                else:
                    cb(self.address, positions)
        except Exception as e:
            logger.error(f"轮询仓位错误: {e}")

    async def _initialize_ws(self):
        try:
            if self.ws_session is None or self.ws_session.closed:
                self.ws_session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=20, connect=8),
                    trust_env=True,
                    headers={"User-Agent": "perpdex-nado-aave/1.0-spot (+aiohttp)"}
                )
            self.subscriptions_ws_connection = await self.ws_session.ws_connect(self.subscriptions_ws, compress=15)
            self.ws_initialized = True
            self._ws_listener_task_handle = asyncio.create_task(self._ws_listener())
            self._ws_ping_task_handle = asyncio.create_task(self._ws_ping_task())
        except Exception as e:
            logger.warning(f"WebSocket 不可用，使用 REST 轮询: {e}")
            self.ws_initialized = False

    async def _ws_ping_task(self):
        while (not self._closing) and self.ws_initialized and self.subscriptions_ws_connection:
            try:
                await asyncio.sleep(25)
                if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                    await self.subscriptions_ws_connection.ping()
            except (asyncio.CancelledError, Exception):
                break

    async def _ws_listener(self):
        while (not self._closing) and self.ws_initialized and self.subscriptions_ws_connection:
            try:
                msg = await self.subscriptions_ws_connection.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_ws_message(json.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
            except Exception as e:
                if not self._closing:
                    logger.error(f"WS 监听错误: {e}")
                break

    async def _handle_ws_message(self, data: dict):
        if data.get('type') == 'best_bid_offer':
            bid = self._from_x18(int(data.get('bid_x18', '0') or 0))
            ask = self._from_x18(int(data.get('ask_x18', '0') or 0))
            mark = (bid + ask) / 2 if (bid and ask) else bid or ask
            if mark > 0 and 'market_stats' in self.callbacks and self.callbacks['market_stats']:
                cb = self.callbacks['market_stats']
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb(str(self.market_id), {'mark_price': mark, 'best_bid': bid, 'best_ask': ask}))
                else:
                    cb(str(self.market_id), {'mark_price': mark, 'best_bid': bid, 'best_ask': ask})

    async def _subscribe_market_stats(self):
        try:
            msg = {
                "method": "subscribe",
                "stream": {"type": "best_bid_offer", "product_id": self.product_id},
                "id": 1
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(msg)
        except Exception as e:
            logger.error(f"订阅行情失败: {e}")

    async def _subscribe_orders(self):
        try:
            sender = self._get_sender_bytes32()
            msg = {
                "method": "subscribe",
                "stream": {"type": "order_update", "subaccount": sender, "product_id": self.product_id},
                "id": 2
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(msg)
        except Exception as e:
            logger.error(f"订阅订单失败: {e}")

    async def _subscribe_positions(self):
        try:
            sender = self._get_sender_bytes32()
            msg = {
                "method": "subscribe",
                "stream": {"type": "position_change", "subaccount": sender, "product_id": self.product_id},
                "id": 3
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(msg)
        except Exception as e:
            logger.error(f"订阅仓位失败: {e}")

    async def close(self):
        self._closing = True
        self.ws_initialized = False
        for task in (self._rest_polling_task_handle, self._ws_listener_task_handle, self._ws_ping_task_handle):
            if task and not task.done():
                task.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(
                *(t for t in (self._rest_polling_task_handle, self._ws_listener_task_handle, self._ws_ping_task_handle) if t),
                return_exceptions=True
            )
        if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
            await self.subscriptions_ws_connection.close()
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()
        if self.session and not self.session.closed:
            await self.session.close()

    async def initialize_client(self) -> None:
        try:
            data = await self._rest_query("contracts", {})
            self.chain_id = data.get('chain_id')
            self.endpoint_address = data.get('endpoint')
            if not self.chain_id or not self.endpoint_address:
                if self.env == 'mainnet':
                    self.chain_id = 57073
                    self.endpoint_address = "0x05ec92D78ED421f3D3Ada77FFdE167106565974E"
                else:
                    self.chain_id = 763373
                    self.endpoint_address = "0x698D87105274292B5673367DEC81874Ce3633Ac2"

            data = await self._rest_query("all_products", {})
            spot_products = data.get('spot_products', [])
            perp_products = data.get('perp_products', [])
            products = spot_products if spot_products else perp_products
            for p in products:
                pid = p.get('product_id')
                if pid is not None:
                    sym = p.get('symbol', p.get('instrument', p.get('name', ''))) or f"PRODUCT_{pid}"
                    self.PRODUCT_ID_TO_SYMBOL[pid] = sym

            if self.product_id is None:
                target = self.target_symbol.upper().replace("-", "").replace("_", "")
                for p in products:
                    pid = p.get('product_id')
                    sym = (p.get('symbol') or p.get('instrument') or '').upper().replace("-", "").replace("_", "")
                    if pid is not None and target in sym:
                        self.product_id = pid
                        break
                if self.product_id is None and products:
                    self.product_id = products[0].get('product_id')

            if self.product_id is None:
                raise ValueError(
                    f"无法解析 spot product_id，请设置 NADO_SPOT_PRODUCT_ID。"
                    f"目标: {self.target_symbol}"
                )

            for p in products:
                if p.get('product_id') == self.product_id:
                    book = p.get('book_info', {})
                    self.price_increment = self._from_x18(int(book.get('price_increment_x18', '10000000000000000')))
                    self.size_increment = self._from_x18(int(book.get('size_increment', '1000000000000000')))
                    self.min_size = self._from_x18(int(book.get('min_size', '100000000000000000000')))
                    break

            logger.info(
                f"Nado Spot 已初始化: product_id={self.product_id}, "
                f"price_inc={self.price_increment}, size_inc={self.size_increment}"
            )
        except Exception as e:
            logger.error(f"Spot 客户端初始化失败: {e}", exc_info=True)
            raise

    async def create_auth_token(self) -> Tuple[str, str]:
        if self.account and self.address:
            return self.address, None
        return "", "No private key configured"

    async def get_account_info(self) -> dict:
        try:
            account = await self.get_account()
            account['positions'] = await self.get_positions()
            return account
        except Exception as e:
            logger.error(f"get_account_info 错误: {e}", exc_info=True)
            return {}
