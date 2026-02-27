#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Order format converter focused on Nado format -> CCXT-like format."""

import logging
import os
from typing import Dict, Any, Union, List
from datetime import datetime

logger = logging.getLogger(__name__)
DEFAULT_SYMBOL = os.getenv("NADO_SYMBOL", "UNKNOWN")


def normalize_order_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    if is_nado_order(order):
        return convert_nado_to_ccxt(order)
    logger.warning(f"Unknown order format: {order}")
    return convert_unknown_to_ccxt(order)


def is_nado_order(order: Dict[str, Any]) -> bool:
    has_price = 'price_x18' in order or 'priceX18' in order or 'price' in order
    has_sender = 'sender' in order or 'subaccount' in order
    return (
        'product_id' in order and
        has_sender and
        has_price and
        'amount' in order and
        ('expiration' in order or 'digest' in order)
    )


def convert_nado_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    try:
        status_mapping = {
            'open': 'open',
            'filled': 'closed',
            'partially_filled': 'open',
            'partially filled': 'open',
            'Partially Filled': 'open',
            'canceled': 'canceled',
            'cancelled': 'canceled',
            'expired': 'expired',
            'rejected': 'rejected'
        }

        order_type_mapping = {
            'default': 'limit',
            'ioc': 'limit',
            'fok': 'limit',
            'post_only': 'limit'
        }
        tif_mapping = {
            'default': 'GTC',
            'ioc': 'IOC',
            'fok': 'FOK',
            'post_only': 'GTC'
        }

        x18 = 10 ** 18
        price_x18 = int(order.get('price_x18') or order.get('priceX18') or order.get('price') or '0')
        amount_raw = int(order.get('amount', '0'))
        unfilled_raw = int(order.get('unfilled_amount') or order.get('unfilledAmount') or amount_raw)

        price = price_x18 / x18
        amount = abs(amount_raw) / x18
        remaining = abs(unfilled_raw) / x18
        filled = amount - remaining
        side = 'buy' if amount_raw > 0 else 'sell'

        nado_order_type = order.get('order_type', 'default')

        placed_at = order.get('placed_at', 0)
        timestamp_ms = placed_at * 1000 if placed_at else None
        datetime_str = (
            datetime.fromtimestamp(placed_at).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if placed_at
            else None
        )

        product_id = order.get('product_id', 0)
        product_symbol_map = {
            26: 'AAVEUSDT0',
            2: 'BTC-PERP',
            8: 'SOL-PERP',
            10: 'XRP-PERP',
            14: 'BNB-PERP',
        }
        symbol = product_symbol_map.get(product_id, f'PRODUCT_{product_id}')

        order_id = order.get('digest', '') or str(order.get('nonce', ''))

        return {
            'id': order_id,
            'clientOrderId': str(int(order.get('nonce', 0)) & ((1 << 20) - 1)) if order.get('nonce') else '',
            'datetime': datetime_str,
            'timestamp': timestamp_ms,
            'lastTradeTimestamp': None,
            'status': status_mapping.get(order.get('status', 'open'), 'open'),
            'symbol': symbol,
            'type': order_type_mapping.get(nado_order_type, 'limit'),
            'timeInForce': tif_mapping.get(nado_order_type, 'GTC'),
            'side': side,
            'price': price,
            'average': price,
            'amount': amount,
            'filled': filled,
            'remaining': remaining,
            'cost': filled * price,
            'trades': [],
            'fee': {},
            'reduceOnly': False,
            'postOnly': nado_order_type == 'post_only',
            'info': order,
        }

    except Exception as e:
        logger.error(f"Error converting Nado order to CCXT: {e}", exc_info=True)
        return {
            'id': order.get('digest', ''),
            'clientOrderId': str(order.get('nonce', '')),
            'status': 'unknown',
            'symbol': DEFAULT_SYMBOL,
            'side': 'buy',
            'price': 0,
            'amount': 0,
            'filled': 0,
            'remaining': 0,
            'cost': 0,
            'info': order,
            'error': str(e)
        }


def convert_unknown_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'id': str(order.get('id', order.get('order_id', order.get('client_order_id', '')))),
        'clientOrderId': str(order.get('client_order_id', order.get('clientOrderId', ''))),
        'status': 'unknown',
        'symbol': DEFAULT_SYMBOL,
        'side': 'buy',
        'price': float(order.get('price', 0)),
        'amount': float(order.get('amount', order.get('size', 0))),
        'filled': float(order.get('filled', order.get('traded_size', 0))),
        'remaining': 0,
        'cost': 0,
        'info': order
    }


def normalize_orders_list(orders: Union[List[Dict], Dict[str, Any]]) -> List[Dict[str, Any]]:
    if isinstance(orders, dict):
        normalized = []
        for market_orders in orders.values():
            if isinstance(market_orders, list):
                for order in market_orders:
                    normalized.append(normalize_order_to_ccxt(order))
        return normalized
    if isinstance(orders, list):
        return [normalize_order_to_ccxt(order) for order in orders]
    logger.warning(f"Unknown orders format: {type(orders)}")
    return []
