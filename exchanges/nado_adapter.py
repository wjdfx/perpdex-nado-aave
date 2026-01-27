"""
Nado Exchange Adapter

Implements the ExchangeInterface for nado.xyz perp DEX.
Uses EIP712 signing with private key authentication.
"""

import asyncio
import json
import logging
import os
import time
import random
from typing import List, Tuple, Dict, Callable, Any, Optional
from decimal import Decimal

import aiohttp
import pandas as pd
from eth_account import Account
from eth_account.messages import encode_typed_data

from .interfaces import ExchangeInterface
from .order_converter import normalize_order_to_ccxt, normalize_orders_list

logger = logging.getLogger(__name__)

# Constants for x18 precision
X18 = 10 ** 18


class NadoAdapter(ExchangeInterface):
    """
    Nado exchange adapter implementing the ExchangeInterface.
    Uses EIP712 signing with private key for authentication.
    """

    # Market ID to Nado product_id mapping
    MARKET_ID_TO_PRODUCT = {
        0: 4,   # ETH-PERP (default)
        1: 2,   # BTC-PERP
        2: 8,   # SOL-PERP
        3: 10,  # XRP-PERP
        4: 14,  # BNB-PERP
    }

    # Product ID to symbol mapping
    PRODUCT_ID_TO_SYMBOL = {
        0: "USDT0",
        1: "KBTC",
        2: "BTC-PERP",
        3: "WETH",
        4: "ETH-PERP",
        5: "USDC",
        8: "SOL-PERP",
        10: "XRP-PERP",
        14: "BNB-PERP",
        16: "HYPE-PERP",
        18: "ZEC-PERP",
        20: "MON-PERP",
        22: "FARTCOIN-PERP",
    }

    def __init__(
        self,
        market_id: int = 0,
        product_id: int = None,  # If None, will use market_id mapping
        subaccount_name: str = "default"
    ):
        self.market_id = market_id
        self.product_id = product_id or self.MARKET_ID_TO_PRODUCT.get(market_id, 4)
        self.subaccount_name = subaccount_name

        # Load configuration from environment
        self.private_key = os.getenv('NADO_PRIVATE_KEY', '')
        self.env = os.getenv('NADO_ENV', 'testnet').lower()

        # Set endpoints based on environment
        # Mainnet: Ink chain, Testnet: Ink Sepolia
        if self.env == 'mainnet':
            # Mainnet endpoints (Ink)
            self.gateway_rest = os.getenv('NADO_GATEWAY_REST', 'https://gateway.prod.nado.xyz/v1')
            self.gateway_ws = os.getenv('NADO_GATEWAY_WS', 'wss://gateway.prod.nado.xyz/v1/ws')
            self.subscriptions_ws = os.getenv('NADO_SUBSCRIPTIONS_WS', 'wss://gateway.prod.nado.xyz/v1/subscribe')
            self.archive_url = os.getenv('NADO_ARCHIVE_URL', 'https://archive.prod.nado.xyz/v1')
            self.trigger_url = os.getenv('NADO_TRIGGER_URL', 'https://trigger.prod.nado.xyz/v1')
        else:
            # Testnet endpoints (Ink Sepolia)
            self.gateway_rest = os.getenv('NADO_GATEWAY_REST', 'https://gateway.test.nado.xyz/v1')
            self.gateway_ws = os.getenv('NADO_GATEWAY_WS', 'wss://gateway.test.nado.xyz/v1/ws')
            self.subscriptions_ws = os.getenv('NADO_SUBSCRIPTIONS_WS', 'wss://gateway.test.nado.xyz/v1/subscribe')
            self.archive_url = os.getenv('NADO_ARCHIVE_URL', 'https://archive.test.nado.xyz/v1')
            self.trigger_url = os.getenv('NADO_TRIGGER_URL', 'https://trigger.test.nado.xyz/v1')

        # Initialize account from private key
        if self.private_key:
            self.account = Account.from_key(self.private_key)
            self.address = self.account.address
        else:
            self.account = None
            self.address = None

        # Session management
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_session: Optional[aiohttp.ClientSession] = None
        self.ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self.subscriptions_ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None

        # Callbacks for subscriptions
        self.callbacks: Dict[str, Callable] = {}
        self.ws_initialized = False

        # Contract info (will be fetched during initialization)
        self.chain_id: Optional[int] = None
        self.endpoint_address: Optional[str] = None
        self.order_digests: Dict[str, str] = {}  # client_order_id -> digest mapping

        # Product trading parameters (will be fetched during initialization)
        self.price_increment: float = 0.1  # Default: $0.1
        self.size_increment: float = 0.001  # Default: 0.001
        self.min_size: float = 100.0  # Default: $100 notional

        logger.info(f"Nado Adapter initialized with env={self.env}, product_id={self.product_id}")

    def _get_sender_bytes32(self) -> str:
        """
        Generate sender bytes32 (address + subaccount identifier).
        Format: address (20 bytes) + subaccount_name padded to 12 bytes
        """
        if not self.address:
            raise ValueError("No address available - private key not set")

        # Convert subaccount name to bytes and pad to 12 bytes
        subaccount_bytes = self.subaccount_name.encode('utf-8')[:12].ljust(12, b'\x00')
        subaccount_hex = subaccount_bytes.hex()

        # Address without 0x prefix + subaccount hex
        return f"0x{self.address[2:].lower()}{subaccount_hex}"

    def _gen_order_nonce(self, recv_time_offset_ms: int = 50) -> int:
        """
        Generate order nonce.
        Most significant 44 bits: recv_time in milliseconds
        Least significant 20 bits: random integer
        """
        timestamp_ms = int(time.time() * 1000)
        recv_time = timestamp_ms + recv_time_offset_ms
        random_int = random.randint(0, (1 << 20) - 1)
        return (recv_time << 20) + random_int

    def _gen_order_verifying_contract(self, product_id: int) -> str:
        """Generate the order verifying contract address based on product ID."""
        be_bytes = product_id.to_bytes(20, byteorder="big", signed=False)
        return "0x" + be_bytes.hex()

    def _to_x18(self, value: float) -> int:
        """Convert a float to x18 precision integer."""
        return int(Decimal(str(value)) * X18)

    def _from_x18(self, value: int) -> float:
        """Convert x18 precision integer to float."""
        return float(Decimal(str(value)) / X18)

    def _round_price_x18(self, price_x18: int) -> int:
        """Round price_x18 to valid increment (ensures divisibility)."""
        # price_increment_x18 = 0.1 * 10^18 = 100000000000000000
        price_increment_x18 = self._to_x18(self.price_increment)
        if price_increment_x18 > 0:
            return (price_x18 // price_increment_x18) * price_increment_x18
        return price_x18

    def _round_size_x18(self, size_x18: int) -> int:
        """Round size_x18 to valid increment."""
        size_increment_x18 = self._to_x18(self.size_increment)
        if size_increment_x18 > 0:
            return (size_x18 // size_increment_x18) * size_increment_x18
        return size_x18

    def _build_appendix(
        self,
        order_type: int = 3,  # POST_ONLY by default
        isolated: bool = False,
        reduce_only: bool = False,
        version: int = 1
    ) -> int:
        """
        Build order appendix.
        Bit layout:
        - Version (8 bits, 0–7): protocol version (currently 1)
        - Isolated (1 bit, 8): whether isolated margin
        - Order Type (2 bits, 9–10): 0=DEFAULT, 1=IOC, 2=FOK, 3=POST_ONLY
        - Reduce Only (1 bit, 11): only decreases existing position
        """
        appendix = version  # bits 0-7
        if isolated:
            appendix |= (1 << 8)  # bit 8
        appendix |= (order_type << 9)  # bits 9-10
        if reduce_only:
            appendix |= (1 << 11)  # bit 11
        return appendix

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={
                    'Content-Type': 'application/json',
                    'Accept-Encoding': 'gzip, deflate, br'
                }
            )
        return self.session

    async def _fetch_contracts(self) -> Dict:
        """Fetch contract information including chain_id and endpoint address."""
        try:
            session = await self._get_session()
            url = f"{self.gateway_rest}/query?type=contracts"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'success':
                        return data.get('data', {})
                    else:
                        logger.warning(f"Contracts query failed: {data.get('error', 'Unknown error')}")
                else:
                    logger.warning(f"Contracts query HTTP error: {response.status}")
        except Exception as e:
            logger.error(f"Failed to fetch contracts: {e}")
        return {}

    def _sign_typed_data(
        self,
        primary_type: str,
        types: Dict,
        message: Dict,
        verifying_contract: str
    ) -> str:
        """Sign EIP712 typed data."""
        if not self.account:
            raise ValueError("No account available - private key not set")

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
        signed = self.account.sign_message(signable)
        return signed.signature.hex()

    async def _rest_query(self, query_type: str, params: Dict = None) -> Dict:
        """Execute REST query."""
        try:
            session = await self._get_session()
            query_params = {"type": query_type}
            if params:
                query_params.update(params)

            url = f"{self.gateway_rest}/query"
            logger.debug(f"REST query: {url} params={query_params}")
            
            async with session.get(url, params=query_params) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"REST query HTTP error {response.status}: {text[:200]}")
                    return {}
                    
                data = await response.json()
                if data.get('status') == 'success':
                    return data.get('data', {})
                else:
                    logger.error(f"Query {query_type} failed: {data.get('error', 'Unknown error')} (code: {data.get('error_code', 'N/A')})")
                    return {}
        except Exception as e:
            logger.error(f"REST query error ({query_type}): {e}")
            return {}

    async def _rest_execute(self, payload: Dict) -> Dict:
        """Execute REST execute command."""
        try:
            session = await self._get_session()
            url = f"{self.gateway_rest}/execute"
            async with session.post(url, json=payload) as response:
                data = await response.json()
                return data
        except Exception as e:
            logger.error(f"REST execute error: {e}")
            return {"status": "failure", "error": str(e)}

    async def place_single_order(self, is_ask: bool, price: float, amount: float) -> Tuple[bool, str]:
        """
        Place single limit order.
        Returns: (success, order_id/client_order_id)
        """
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()
            expiration = int(time.time()) + 86400 * 30  # 30 days expiration

            # Convert to x18 precision first
            price_x18 = self._to_x18(price)
            amount_x18 = self._to_x18(amount)
            
            # Round to valid increments at x18 level (ensures exact divisibility)
            price_x18 = self._round_price_x18(price_x18)
            amount_x18 = self._round_size_x18(amount_x18)
            
            # Amount: positive for buy, negative for sell
            if is_ask:
                amount_x18 = -amount_x18
            
            logger.debug(f"Placing order: is_ask={is_ask}, price_x18={price_x18}, amount_x18={amount_x18}")

            # Build appendix (POST_ONLY by default)
            appendix = self._build_appendix(order_type=3)  # POST_ONLY

            # Order message for signing
            order_message = {
                "sender": sender,
                "priceX18": str(price_x18),
                "amount": str(amount_x18),
                "expiration": str(expiration),
                "nonce": str(nonce),
                "appendix": str(appendix)
            }

            # EIP712 types for Order
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

            # Sign the order
            verifying_contract = self._gen_order_verifying_contract(self.product_id)
            signature = self._sign_typed_data(
                "Order",
                order_types,
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

            # Build request payload
            payload = {
                "place_order": {
                    "product_id": self.product_id,
                    "order": order_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }

            # Execute order
            response = await self._rest_execute(payload)

            if response.get('status') == 'success':
                digest = response.get('data', {}).get('digest', '')
                # Use nonce as client order id
                client_order_id = str(nonce & ((1 << 20) - 1))  # Use last 20 bits as ID
                self.order_digests[client_order_id] = digest
                logger.info(f"Order placed successfully: digest={digest}")
                return True, client_order_id
            else:
                logger.error(f"Failed to place order: {response.get('error', 'Unknown error')}")
                return False, ''
        except Exception as e:
            logger.error(f"place_single_order error: {e}", exc_info=True)
            return False, ''

    async def place_multi_orders(self, orders: List[Tuple[bool, float, float]]) -> Tuple[bool, List[str]]:
        """
        Place multiple limit orders.
        orders: [(is_ask, price, amount), ...]
        Returns: (success, order_ids)
        """
        if not orders:
            logger.warning("place_multi_orders: No orders provided")
            return True, []

        try:
            order_ids = []
            for i, (is_ask, price, amount) in enumerate(orders):
                logger.debug(f"place_multi_orders: Placing order {i+1}/{len(orders)}: is_ask={is_ask}, price={price}, amount={amount}")
                success, order_id = await self.place_single_order(is_ask, price, amount)
                if not success:
                    logger.error(f"place_multi_orders: Failed to place order {i+1}")
                    # Cancel previously placed orders
                    if order_ids:
                        logger.info(f"place_multi_orders: Cancelling {len(order_ids)} previously placed orders")
                        await self.cancel_grid_orders(order_ids)
                    return False, []
                order_ids.append(order_id)

            logger.debug(f"place_multi_orders: Successfully placed {len(order_ids)} orders")
            return True, order_ids
        except Exception as e:
            logger.error(f"place_multi_orders error: {e}")
            return False, []

    async def place_single_market_order(self, is_ask: bool, price: float, amount: float) -> Tuple[bool, str]:
        """
        Place single market order (using IOC order type).
        """
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()
            expiration = int(time.time()) + 60  # 1 minute expiration for market order

            # For market orders, use a very high/low price depending on side
            market_price = price * 1.1 if not is_ask else price * 0.9
            
            # Convert to x18 and round
            price_x18 = self._round_price_x18(self._to_x18(market_price))
            amount_x18 = self._round_size_x18(self._to_x18(amount))
            
            if is_ask:
                amount_x18 = -amount_x18

            # Build appendix with IOC order type
            appendix = self._build_appendix(order_type=1)  # IOC

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
                "Order",
                order_types,
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
                return True, client_order_id
            else:
                logger.error(f"Failed to place market order: {response.get('error', 'Unknown error')}")
                return False, ''
        except Exception as e:
            logger.error(f"place_single_market_order error: {e}", exc_info=True)
            return False, ''

    async def cancel_grid_orders(self, order_ids: List[str]) -> bool:
        """
        Batch cancel orders.
        """
        if not order_ids:
            logger.warning("cancel_grid_orders: No order_ids provided")
            return True

        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()

            # Get digests for order_ids
            digests = []
            for order_id in order_ids:
                digest = self.order_digests.get(order_id)
                if digest:
                    digests.append(digest)
                else:
                    logger.warning(f"No digest found for order_id {order_id}")

            if not digests:
                # Try to cancel all orders for the product
                logger.info("No digests found, cancelling all orders for product")
                return await self._cancel_product_orders()

            # Cancellation message
            cancel_message = {
                "sender": sender,
                "productIds": [self.product_id] * len(digests),
                "digests": digests,
                "nonce": str(nonce)
            }

            # EIP712 types for Cancellation
            cancel_types = {
                "Cancellation": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "productIds", "type": "uint32[]"},
                    {"name": "digests", "type": "bytes32[]"},
                    {"name": "nonce", "type": "uint64"}
                ]
            }

            # Sign the cancellation
            signature = self._sign_typed_data(
                "Cancellation",
                cancel_types,
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
                # Remove cancelled orders from tracking
                for order_id in order_ids:
                    self.order_digests.pop(order_id, None)
                logger.info(f"Successfully cancelled {len(digests)} orders")
                return True
            else:
                logger.error(f"Failed to cancel orders: {response.get('error', 'Unknown error')}")
                return False
        except Exception as e:
            logger.error(f"cancel_grid_orders error: {e}", exc_info=True)
            return False

    async def _cancel_product_orders(self) -> bool:
        """Cancel all orders for the current product."""
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()

            cancel_message = {
                "sender": sender,
                "productIds": [self.product_id],
                "nonce": str(nonce)
            }

            cancel_types = {
                "CancellationProducts": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "productIds", "type": "uint32[]"},
                    {"name": "nonce", "type": "uint64"}
                ]
            }

            signature = self._sign_typed_data(
                "CancellationProducts",
                cancel_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "productIds": [self.product_id],
                    "nonce": nonce
                },
                self.endpoint_address
            )

            payload = {
                "cancel_product_orders": {
                    "tx": cancel_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }

            response = await self._rest_execute(payload)
            return response.get('status') == 'success'
        except Exception as e:
            logger.error(f"_cancel_product_orders error: {e}")
            return False

    async def modify_grid_order(self, order_id: str, new_price: float, new_amount: float) -> bool:
        """
        Modify order - Nado does not support order modification directly.
        Need to cancel and create new order.
        """
        logger.warning("Nado does not support order modification. Use cancel and create new order instead.")
        return False

    async def get_orders(self) -> List[dict]:
        """Get current orders."""
        try:
            sender = self._get_sender_bytes32()
            params = {
                "sender": sender,
                "product_id": self.product_id
            }
            data = await self._rest_query("subaccount_orders", params)
            orders = data.get('orders', [])
            
            # Log raw order format for debugging (only first order)
            if orders and len(orders) > 0:
                logger.debug(f"Raw Nado order sample: {orders[0]}")
            
            return orders
        except Exception as e:
            logger.error(f"get_orders error: {e}", exc_info=True)
            return []

    async def get_trades(self, limit: int = 1) -> List[dict]:
        """Get recent trades."""
        # Note: Nado uses indexer API for historical trades
        # For recent fills, we can use the fills stream or query
        try:
            # This would require the archive/indexer API
            # For now, return empty list as trades are handled via subscriptions
            return []
        except Exception as e:
            logger.error(f"get_trades error: {e}")
            return []

    async def get_positions(self) -> Dict[str, dict]:
        """Get positions."""
        try:
            sender = self._get_sender_bytes32()
            params = {"subaccount": sender}
            data = await self._rest_query("subaccount_info", params)

            positions = {}
            perp_balances = data.get('perp_balances', [])
            perp_products = data.get('perp_products', [])

            for balance in perp_balances:
                product_id = balance.get('product_id')
                amount = self._from_x18(int(balance.get('balance', {}).get('amount', '0')))
                v_quote = self._from_x18(int(balance.get('balance', {}).get('v_quote_balance', '0')))

                # Find product info
                product_info = next((p for p in perp_products if p.get('product_id') == product_id), {})
                oracle_price = self._from_x18(int(product_info.get('oracle_price_x18', '0')))

                if amount != 0:
                    symbol = self.PRODUCT_ID_TO_SYMBOL.get(product_id, f"PRODUCT_{product_id}")
                    positions[symbol] = {
                        'instrument': symbol,
                        'product_id': product_id,
                        'size': amount,
                        'sign': 1 if amount > 0 else (-1 if amount < 0 else 0),
                        'notional': abs(amount * oracle_price),
                        'entry_price': -v_quote / amount if amount != 0 else 0,
                        'mark_price': oracle_price,
                        'unrealized_pnl': amount * oracle_price + v_quote if amount != 0 else 0
                    }

            return positions
        except Exception as e:
            logger.error(f"get_positions error: {e}", exc_info=True)
            return {}

    async def get_account(self) -> dict:
        """Get account info including collateral."""
        try:
            sender = self._get_sender_bytes32()
            params = {"subaccount": sender}
            data = await self._rest_query("subaccount_info", params)

            healths = data.get('healths', [{}])
            unweighted_health = healths[2] if len(healths) > 2 else {}

            total_equity = self._from_x18(int(unweighted_health.get('assets', '0')))
            total_liability = self._from_x18(int(unweighted_health.get('liabilities', '0')))

            return {
                'total_equity': total_equity,
                'collateral': total_equity - total_liability,
                'free_collateral': self._from_x18(int(healths[0].get('health', '0'))) if healths else 0
            }
        except Exception as e:
            logger.error(f"get_account error: {e}", exc_info=True)
            return {}

    async def candle_stick(self, market_id: int, resolution: str, count_back: int = 200) -> pd.DataFrame:
        """Get candlestick data."""
        try:
            # Nado uses indexer API for candlesticks
            # Map resolution to granularity in seconds
            resolution_map = {
                '1m': 60,
                '5m': 300,
                '15m': 900,
                '1h': 3600,
                '4h': 14400,
                '1d': 86400
            }
            granularity = resolution_map.get(resolution, 60)

            product_id = self.MARKET_ID_TO_PRODUCT.get(market_id, self.product_id)

            # Use archive API for candlesticks
            # Note: This would require the archive endpoint to be configured
            # For now, return empty DataFrame
            logger.warning("Candlestick data requires archive/indexer API")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"candle_stick error: {e}")
            return pd.DataFrame()

    async def modify_order(self, order_id: int, new_price: float, new_amount: float) -> bool:
        """Modify order - NOT SUPPORTED by Nado API."""
        logger.warning("Nado does not support order modification.")
        return False

    async def get_orders_by_rest(self) -> List[dict]:
        """Get orders via REST API."""
        return await self.get_orders()

    async def get_trades_by_rest(self, ask_filter: int, limit: int) -> List[dict]:
        """Get trades via REST API."""
        return await self.get_trades(limit)

    async def subscribe(self, callbacks: Dict[str, Callable[[str, Any], None]], proxy: str = None) -> None:
        """
        Subscribe to events.
        callbacks: {
            'market_stats': func(market_id, stats),
            'orders': func(account_id, orders),
            'positions': func(account_id, positions)
        }
        """
        self.callbacks = callbacks
        self.proxy = proxy

        if proxy:
            logger.warning(f"Nado adapter: Proxy support ({proxy}) may not be fully implemented.")

        # First, poll market price immediately to get initial price
        if 'market_stats' in callbacks:
            logger.info("Fetching initial market price...")
            initial_price = await self._poll_market_price()
            if initial_price:
                logger.info(f"Initial market price: {initial_price}")
            else:
                logger.warning("Could not fetch initial market price")

        # Try to initialize WebSocket connection
        if not self.ws_initialized:
            await self._initialize_ws()

        # If WebSocket is available, subscribe to streams
        if self.ws_initialized and self.subscriptions_ws_connection:
            if 'market_stats' in callbacks:
                await self._subscribe_market_stats()

            if 'orders' in callbacks:
                await self._subscribe_orders()
                # Also subscribe to fills to detect order executions
                await self._subscribe_fills()

            if 'positions' in callbacks:
                await self._subscribe_positions()

            logger.info(f"Nado subscribe: Registered WebSocket callbacks for {list(callbacks.keys())}")
        else:
            # WebSocket not available, start REST polling task
            logger.info("WebSocket not available, starting REST API polling for market data")
        
        # Always start REST polling as a backup (even if WebSocket works)
        # This ensures we always have price updates
        asyncio.create_task(self._rest_polling_task())

    async def _rest_polling_task(self):
        """Poll REST API for updates (backup for WebSocket)."""
        poll_count = 0
        while True:
            try:
                # Poll market price every cycle
                await self._poll_market_price()
                
                # ALWAYS poll orders to detect fills
                # WebSocket fill events depend on order_digests which is lost on restart
                # REST polling ensures we detect order changes even after restart
                poll_count += 1
                if poll_count % 2 == 0:
                    await self._poll_orders()
                    await self._check_for_fills()  # Check for order disappearances (fills)
                    await self._poll_positions()
                
                await asyncio.sleep(3)  # Poll every 3 seconds
            except Exception as e:
                logger.error(f"REST polling error: {e}")
                await asyncio.sleep(5)

    async def _poll_orders(self):
        """Poll orders and trigger callback."""
        try:
            if 'orders' not in self.callbacks or not self.callbacks['orders']:
                return
            
            orders = await self.get_orders()
            if orders:
                # Normalize orders to CCXT format
                normalized_orders = normalize_orders_list(orders)
                logger.debug(f"Polled {len(orders)} orders, normalized {len(normalized_orders)}")
                
                callback = self.callbacks['orders']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(self.address, normalized_orders))
                else:
                    callback(self.address, normalized_orders)
        except Exception as e:
            logger.error(f"Poll orders error: {e}")

    async def _poll_positions(self):
        """Poll positions and trigger callback."""
        try:
            if 'positions' not in self.callbacks or not self.callbacks['positions']:
                return
            
            positions = await self.get_positions()
            if positions:
                callback = self.callbacks['positions']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(self.address, positions))
                else:
                    callback(self.address, positions)
        except Exception as e:
            logger.error(f"Poll positions error: {e}")

    async def _check_for_fills(self):
        """Check for order disappearances which indicate fills.
        
        This method tracks orders between polls and detects when orders
        disappear (meaning they were filled or cancelled).
        When an order disappears, we trigger a 'filled' callback.
        """
        try:
            if 'orders' not in self.callbacks or not self.callbacks['orders']:
                return
            
            # Initialize tracking dict if not exists
            if not hasattr(self, '_last_known_orders'):
                self._last_known_orders = {}
            
            # Get current orders
            current_orders = await self.get_orders()
            if current_orders is None:
                return
            
            # Build current orders dict: digest -> order_info
            current_order_digests = {}
            for order in current_orders:
                digest = order.get('digest', '')
                if digest:
                    current_order_digests[digest] = order
            
            # Find disappeared orders (were in last poll, not in current)
            disappeared_orders = []
            for digest, order_info in self._last_known_orders.items():
                if digest not in current_order_digests:
                    # Order disappeared - likely filled
                    disappeared_orders.append((digest, order_info))
            
            # Update tracking
            self._last_known_orders = current_order_digests
            
            if not disappeared_orders:
                return
            
            # Process disappeared orders as fills
            fills = []
            for digest, order_info in disappeared_orders:
                # Try to find client_order_id from our tracking
                client_order_id = ''
                for cid, d in self.order_digests.items():
                    if d == digest:
                        client_order_id = cid
                        break
                
                if not client_order_id:
                    # Use a shortened digest as fallback ID
                    client_order_id = digest[:10] if len(digest) > 10 else digest
                    logger.debug(f"No client_order_id for disappeared order {digest}, using shortened digest")
                
                # Parse order info
                is_bid = order_info.get('is_bid', True)
                price_x18 = order_info.get('price_x18', '0')
                amount_x18 = order_info.get('initial_amount_x18', order_info.get('amount_x18', '0'))
                
                X18 = 10 ** 18
                price = int(price_x18) / X18 if price_x18 else 0
                amount = abs(int(amount_x18)) / X18 if amount_x18 else 0
                
                # Create filled order in CCXT format
                ccxt_order = {
                    'id': client_order_id,
                    'clientOrderId': client_order_id,
                    'status': 'closed',  # CCXT uses 'closed' for filled
                    'symbol': f'PRODUCT_{self.product_id}',
                    'side': 'buy' if is_bid else 'sell',
                    'price': price,
                    'amount': amount,
                    'filled': amount,
                    'remaining': 0,
                    'cost': amount * price,
                    'info': order_info,
                }
                fills.append(ccxt_order)
                logger.info(f"🔥 Detected fill (order disappeared): client_id={client_order_id}, "
                           f"side={'buy' if is_bid else 'sell'}, price={price}, amount={amount}")
            
            # Trigger callback for fills
            if fills:
                logger.info(f"🔥 Triggering orders callback for {len(fills)} disappeared orders")
                callback = self.callbacks['orders']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(self.address, fills))
                else:
                    callback(self.address, fills)
                    
        except Exception as e:
            logger.error(f"_check_for_fills error: {e}", exc_info=True)

    async def _poll_market_price(self):
        """Poll market price once and trigger callback."""
        try:
            # Use market_price (singular) query
            prices = await self._rest_query("market_price", {"product_id": self.product_id})
            if prices:
                # Nado returns bid_x18 and ask_x18
                bid_x18 = prices.get('bid_x18', '0')
                ask_x18 = prices.get('ask_x18', '0')
                
                bid = self._from_x18(int(bid_x18)) if bid_x18 else 0
                ask = self._from_x18(int(ask_x18)) if ask_x18 else 0
                
                # Calculate mark price as mid price
                mark_price = (bid + ask) / 2 if bid and ask else bid or ask
                
                if mark_price > 0:
                    stats = {
                        'mark_price': mark_price,
                        'best_bid': bid,
                        'best_ask': ask
                    }
                    logger.debug(f"Market price polled: mark_price={mark_price}, bid={bid}, ask={ask}")
                    
                    if 'market_stats' in self.callbacks and self.callbacks['market_stats']:
                        callback = self.callbacks['market_stats']
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback(str(self.market_id), stats))
                        else:
                            callback(str(self.market_id), stats)
                    return mark_price
            return None
        except Exception as e:
            logger.error(f"Poll market price error: {e}")
            return None

    async def _initialize_ws(self):
        """Initialize WebSocket connection."""
        try:
            if self.ws_session is None or self.ws_session.closed:
                self.ws_session = aiohttp.ClientSession()

            # Connect to subscriptions WebSocket
            logger.info(f"Connecting to WebSocket: {self.subscriptions_ws}")
            self.subscriptions_ws_connection = await self.ws_session.ws_connect(
                self.subscriptions_ws,
                compress=15  # Enable permessage-deflate
            )

            self.ws_initialized = True
            logger.info("Nado WebSocket initialized successfully")

            # Start listening for messages
            asyncio.create_task(self._ws_listener())

            # Start ping task to keep connection alive
            asyncio.create_task(self._ws_ping_task())

        except aiohttp.ClientResponseError as e:
            logger.error(f"WebSocket connection failed (HTTP {e.status}): {e.message}")
            logger.warning("WebSocket unavailable - will use REST API polling for updates")
            self.ws_initialized = False
        except aiohttp.WSServerHandshakeError as e:
            logger.error(f"WebSocket handshake failed: {e}")
            logger.warning("WebSocket unavailable - will use REST API polling for updates")
            self.ws_initialized = False
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket: {e}")
            logger.warning("WebSocket unavailable - will use REST API polling for updates")
            self.ws_initialized = False

    async def _ws_ping_task(self):
        """Send ping frames every 30 seconds to keep connection alive."""
        while self.ws_initialized and self.subscriptions_ws_connection:
            try:
                await asyncio.sleep(25)
                if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                    await self.subscriptions_ws_connection.ping()
            except Exception as e:
                logger.error(f"WebSocket ping error: {e}")
                break

    async def _ws_listener(self):
        """Listen for WebSocket messages."""
        while self.ws_initialized and self.subscriptions_ws_connection:
            try:
                msg = await self.subscriptions_ws_connection.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_ws_message(data)
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("WebSocket connection closed")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {msg.data}")
                    break
            except Exception as e:
                logger.error(f"WebSocket listener error: {e}")
                break

    async def _handle_ws_message(self, data: dict):
        """Handle incoming WebSocket messages."""
        event_type = data.get('type')

        if event_type == 'best_bid_offer':
            await self._handle_market_stats(data)
        elif event_type == 'order_update':
            await self._handle_order_update(data)
        elif event_type == 'position_change':
            await self._handle_position_change(data)
        elif event_type == 'fill':
            await self._handle_fill(data)

    async def _handle_market_stats(self, data: dict):
        """Handle market stats updates from WebSocket."""
        if 'market_stats' in self.callbacks and self.callbacks['market_stats']:
            # WebSocket best_bid_offer stream returns bid_x18 and ask_x18
            bid_x18 = data.get('bid_x18', data.get('best_bid', '0'))
            ask_x18 = data.get('ask_x18', data.get('best_ask', '0'))
            
            bid = self._from_x18(int(bid_x18)) if bid_x18 else 0
            ask = self._from_x18(int(ask_x18)) if ask_x18 else 0
            mark_price = (bid + ask) / 2 if bid and ask else bid or ask
            
            stats = {
                'mark_price': mark_price,
                'best_bid': bid,
                'best_ask': ask
            }
            
            logger.debug(f"WebSocket market stats: {stats}")
            
            callback = self.callbacks['market_stats']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(str(self.market_id), stats))
            else:
                callback(str(self.market_id), stats)

    async def _handle_order_update(self, data: dict):
        """Handle order updates."""
        if 'orders' in self.callbacks and self.callbacks['orders']:
            orders = [data.get('order', data)]
            normalized_orders = normalize_orders_list(orders)
            callback = self.callbacks['orders']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(self.address, normalized_orders))
            else:
                callback(self.address, normalized_orders)

    async def _handle_position_change(self, data: dict):
        """Handle position changes."""
        if 'positions' in self.callbacks and self.callbacks['positions']:
            positions = await self.get_positions()
            callback = self.callbacks['positions']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(self.address, positions))
            else:
                callback(self.address, positions)

    async def _handle_fill(self, data: dict):
        """Handle fill events (order filled)."""
        logger.info(f"Fill event received: {data}")
        
        if 'orders' in self.callbacks and self.callbacks['orders']:
            # Extract fill information from the actual fill event format
            # Fill event fields: order_digest, price, filled_qty, remaining_qty, original_qty, is_bid, etc.
            digest = data.get('order_digest', '')
            price_x18 = data.get('price', '0')
            filled_qty = data.get('filled_qty', '0')
            original_qty = data.get('original_qty', '0')
            remaining_qty = data.get('remaining_qty', '0')
            is_bid = data.get('is_bid', True)
            product_id = data.get('product_id', self.product_id)
            
            # IMPORTANT: Find the client_order_id from digest
            # buy_orders/sell_orders use client_order_id as key, not digest
            client_order_id = ''
            for cid, d in self.order_digests.items():
                if d == digest:
                    client_order_id = cid
                    break
            
            if not client_order_id:
                logger.warning(f"Could not find client_order_id for digest {digest}, using digest as ID")
                client_order_id = digest
            
            # Determine if fully filled or partially filled
            status = 'filled' if remaining_qty == '0' else 'open'
            
            # Amount sign: positive for buy (bid), negative for sell (ask)
            amount_sign = 1 if is_bid else -1
            amount = str(int(original_qty) * amount_sign)
            unfilled = remaining_qty
            
            # Parse price from x18 format
            X18 = 10 ** 18
            price = int(price_x18) / X18
            amount_float = abs(int(original_qty)) / X18
            filled_float = abs(int(original_qty) - int(remaining_qty)) / X18
            
            # Create order in CCXT format directly (bypass converter issues)
            ccxt_order = {
                'id': client_order_id,  # Use client_order_id to match buy_orders/sell_orders
                'clientOrderId': client_order_id,
                'status': 'closed' if status == 'filled' else 'open',  # CCXT uses 'closed' for filled
                'symbol': f'PRODUCT_{product_id}',
                'side': 'buy' if is_bid else 'sell',
                'price': price,
                'amount': amount_float,
                'filled': filled_float,
                'remaining': abs(int(remaining_qty)) / X18,
                'cost': filled_float * price,
                'info': data,
            }
            
            logger.info(f"Processing fill: client_order_id={client_order_id}, digest={digest}, side={'buy' if is_bid else 'sell'}, price={price}, amount={amount_float}, status={ccxt_order['status']}")
            
            callback = self.callbacks['orders']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(self.address, [ccxt_order]))
            else:
                callback(self.address, [ccxt_order])

    async def _subscribe_market_stats(self):
        """Subscribe to best bid/offer stream."""
        try:
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "best_bid_offer",
                    "product_id": self.product_id
                },
                "id": 1
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"Subscribed to best_bid_offer for product {self.product_id}")
        except Exception as e:
            logger.error(f"Failed to subscribe to market stats: {e}")

    async def _subscribe_orders(self):
        """Subscribe to order updates."""
        try:
            sender = self._get_sender_bytes32()
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "order_update",
                    "subaccount": sender,
                    "product_id": self.product_id
                },
                "id": 2
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"Subscribed to order_update for product {self.product_id}")
        except Exception as e:
            logger.error(f"Failed to subscribe to orders: {e}")

    async def _subscribe_positions(self):
        """Subscribe to position changes."""
        try:
            sender = self._get_sender_bytes32()
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "position_change",
                    "subaccount": sender,
                    "product_id": self.product_id
                },
                "id": 3
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"Subscribed to position_change for product {self.product_id}")
        except Exception as e:
            logger.error(f"Failed to subscribe to positions: {e}")

    async def _subscribe_fills(self):
        """Subscribe to fill events (order executions)."""
        try:
            sender = self._get_sender_bytes32()
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "fill",
                    "subaccount": sender,
                    "product_id": self.product_id
                },
                "id": 4
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"Subscribed to fill events for product {self.product_id}")
        except Exception as e:
            logger.error(f"Failed to subscribe to fills: {e}")

    async def close(self):
        """Close connections."""
        self.ws_initialized = False
        try:
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.close()
            if self.ws_session and not self.ws_session.closed:
                await self.ws_session.close()
            if self.session and not self.session.closed:
                await self.session.close()
        except Exception as e:
            logger.error(f"Error closing connections: {e}")

    async def initialize_client(self) -> None:
        """Initialize the exchange client."""
        try:
            # Fetch contract information from API
            contracts = await self._fetch_contracts()
            self.chain_id = contracts.get('chain_id')
            self.endpoint_address = contracts.get('endpoint')

            if not self.chain_id or not self.endpoint_address:
                logger.warning("Could not fetch contract info from API, using hardcoded defaults")
                # Use hardcoded values based on environment
                if self.env == 'mainnet':
                    # Mainnet: Ink chain
                    self.chain_id = 57073  # Ink mainnet chain_id
                    self.endpoint_address = "0x05ec92D78ED421f3D3Ada77FFdE167106565974E"
                else:
                    # Testnet: Ink Sepolia
                    self.chain_id = 763373  # Ink Sepolia chain_id
                    self.endpoint_address = "0x698D87105274292B5673367DEC81874Ce3633Ac2"

            # Fetch product trading parameters
            await self._fetch_product_params()

            logger.info(f"Nado client initialized: env={self.env}, chain_id={self.chain_id}, endpoint={self.endpoint_address}")
            logger.info(f"Product {self.product_id} params: price_increment={self.price_increment}, size_increment={self.size_increment}, min_size={self.min_size}")
        except Exception as e:
            logger.error(f"Failed to initialize client: {e}", exc_info=True)
            # Set fallback values based on environment
            if self.env == 'mainnet':
                self.chain_id = 57073
                self.endpoint_address = "0x05ec92D78ED421f3D3Ada77FFdE167106565974E"
            else:
                self.chain_id = 763373
                self.endpoint_address = "0x698D87105274292B5673367DEC81874Ce3633Ac2"

    async def _fetch_product_params(self) -> None:
        """Fetch product trading parameters (price/size increments, min_size)."""
        try:
            # Query all_products to get trading parameters
            data = await self._rest_query("all_products", {})
            if not data:
                logger.warning("Could not fetch product params, using defaults")
                return

            # Find our product in perp_products
            perp_products = data.get('perp_products', [])
            for product in perp_products:
                if product.get('product_id') == self.product_id:
                    book_info = product.get('book_info', {})
                    
                    # price_increment_x18
                    price_inc_x18 = int(book_info.get('price_increment_x18', '100000000000000000'))
                    self.price_increment = self._from_x18(price_inc_x18)
                    
                    # size_increment (in base units)
                    size_inc = int(book_info.get('size_increment', '1000000000000000'))
                    self.size_increment = self._from_x18(size_inc)
                    
                    # min_size (notional value)
                    min_size = int(book_info.get('min_size', '100000000000000000000'))
                    self.min_size = self._from_x18(min_size)
                    
                    logger.info(f"Fetched product {self.product_id} params: price_inc={self.price_increment}, size_inc={self.size_increment}, min_size={self.min_size}")
                    return

            logger.warning(f"Product {self.product_id} not found in perp_products")
        except Exception as e:
            logger.error(f"Failed to fetch product params: {e}")

    async def create_auth_token(self) -> Tuple[str, str]:
        """
        Create authentication token.
        For Nado, authentication is done via EIP712 signatures.
        """
        if self.account and self.address:
            return self.address, None
        return "", "No private key configured"

    async def get_account_info(self) -> dict:
        """Get detailed account information including positions."""
        try:
            account = await self.get_account()
            positions = await self.get_positions()
            account['positions'] = positions
            return account
        except Exception as e:
            logger.error(f"get_account_info error: {e}", exc_info=True)
            return {}


async def run():
    """Test function for the Nado adapter."""
    from dotenv import load_dotenv
    load_dotenv()

    from common.logging_config import setup_logging
    setup_logging()

    adapter = NadoAdapter(market_id=0)
    await adapter.initialize_client()

    # Test get account info
    account_info = await adapter.get_account_info()
    print("Account Info:", json.dumps(account_info, ensure_ascii=False, indent=2, default=str))

    # Test get orders
    orders = await adapter.get_orders()
    print("Orders:", json.dumps(orders, ensure_ascii=False, indent=2, default=str))

    # Test get positions
    positions = await adapter.get_positions()
    print("Positions:", json.dumps(positions, ensure_ascii=False, indent=2, default=str))

    await adapter.close()


if __name__ == "__main__":
    asyncio.run(run())
