#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Order format converter module for unifying Lighter, GRVT, and StandX order formats to CCXT standard.
"""

import logging
from typing import Dict, Any, Union, List
from datetime import datetime

logger = logging.getLogger(__name__)


def normalize_order_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert exchange-specific order format to CCXT standard format.
    
    Args:
        order: Exchange-specific order dictionary
        
    Returns:
        CCXT standardized order dictionary
    """
    # Determine if this is a Lighter, GRVT, StandX, Nado, or Extended order based on field structure
    if is_lighter_order(order):
        return convert_lighter_to_ccxt(order)
    elif is_grvt_order(order):
        return convert_grvt_to_ccxt(order)
    elif is_standx_order(order):
        return convert_standx_to_ccxt(order)
    elif is_nado_order(order):
        return convert_nado_to_ccxt(order)
    elif is_extended_order(order):
        return convert_extended_to_ccxt(order)
    else:
        logger.warning(f"Unknown order format: {order}")
        return convert_unknown_to_ccxt(order)


def is_lighter_order(order: Dict[str, Any]) -> bool:
    """Check if order is in Lighter format."""
    # Lighter orders have client_order_index, is_ask, filled_base_amount fields
    return (
        'client_order_index' in order and
        'is_ask' in order and
        'filled_base_amount' in order and
        'initial_base_amount' in order
    )


def is_grvt_order(order: Dict[str, Any]) -> bool:
    """Check if order is in GRVT format."""
    # GRVT orders have order_id, legs, state, metadata fields
    return (
        'order_id' in order and
        'legs' in order and
        'state' in order and
        'metadata' in order
    )


def is_standx_order(order: Dict[str, Any]) -> bool:
    """Check if order is in StandX format."""
    # StandX orders have id, cl_ord_id, symbol, side, order_type, status fields
    return (
        'id' in order and
        'cl_ord_id' in order and
        'symbol' in order and
        'side' in order and
        'order_type' in order and
        'status' in order and
        'price' in order and
        'qty' in order
    )


def is_nado_order(order: Dict[str, Any]) -> bool:
    """Check if order is in Nado format."""
    # Nado orders have product_id, sender, price (priceX18/price_x18), amount, expiration, nonce, digest fields
    # Handle both camelCase and snake_case variations
    has_price = 'price_x18' in order or 'priceX18' in order or 'price' in order
    has_sender = 'sender' in order or 'subaccount' in order
    return (
        'product_id' in order and
        has_sender and
        has_price and
        'amount' in order and
        ('expiration' in order or 'digest' in order)
    )


def is_extended_order(order: Dict[str, Any]) -> bool:
    """Check if order is in Extended format."""
    # Extended orders have id, market, type, side, status, qty fields
    # and market is in format "XXX-USD" (e.g., "ETH-USD")
    return (
        'id' in order and
        'market' in order and
        'type' in order and
        'side' in order and
        'status' in order and
        'qty' in order and
        isinstance(order.get('market', ''), str) and
        '-USD' in order.get('market', '')
    )


def convert_lighter_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Lighter order format to CCXT format."""
    try:
        # Map status
        status_mapping = {
            'open': 'open',
            'filled': 'closed', 
            'canceled': 'canceled',
            'expired': 'expired',
            'rejected': 'rejected'
        }
        
        # Map side
        side = 'sell' if order.get('is_ask', False) else 'buy'
        
        # Calculate remaining amount
        amount = float(order.get('initial_base_amount', 0))
        filled = float(order.get('filled_base_amount', 0))
        remaining = amount - filled
        
        # Build CCXT order
        ccxt_order = {
            'id': str(order.get('order_id', order.get('client_order_index', ''))),
            'clientOrderId': str(order.get('client_order_index', '')),
            'datetime': None,  # Lighter doesn't provide timestamp
            'timestamp': None,  # Lighter doesn't provide timestamp
            'lastTradeTimestamp': None,
            'status': status_mapping.get(order.get('status', 'open'), 'open'),
            'symbol': order.get('symbol', 'ETH/USDT'),
            'type': 'limit',  # Lighter orders are typically limit orders
            'timeInForce': 'GTC',  # Good Till Cancelled
            'side': side,
            'price': float(order.get('price', 0)),
            'average': float(order.get('average_price', order.get('price', 0))),
            'amount': amount,
            'filled': filled,
            'remaining': remaining,
            'cost': filled * float(order.get('price', 0)),
            'trades': [],
            'fee': {},
            'reduceOnly': order.get('reduce_only', False),
            'postOnly': order.get('post_only', False),
            'info': order  # Store original order as info
        }
        
        return ccxt_order
        
    except Exception as e:
        logger.error(f"Error converting Lighter order to CCXT: {e}", exc_info=True)
        return {
            'id': str(order.get('client_order_index', '')),
            'clientOrderId': str(order.get('client_order_index', '')),
            'status': 'unknown',
            'symbol': 'ETH/USDT',
            'side': 'buy',
            'price': 0,
            'amount': 0,
            'filled': 0,
            'remaining': 0,
            'cost': 0,
            'info': order,
            'error': str(e)
        }


def convert_grvt_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    """Convert GRVT order format to CCXT format."""
    try:
        # Map status
        status_mapping = {
            'OPEN': 'open',
            'FILLED': 'closed',
            'CANCELED': 'canceled',
            'EXPIRED': 'expired',
            'REJECTED': 'rejected',
            'UNSPECIFIED': 'unknown'
        }
        
        # Get leg information (GRVT orders can have multiple legs, we take the first)
        leg = order.get('legs', [{}])[0] if order.get('legs') else {}
        
        # Map side based on is_buying_asset
        side = 'buy' if leg.get('is_buying_asset', True) else 'sell'
        
        # Get state information
        state = order.get('state', {})
        book_size = float(state.get('book_size', ['0'])[0]) if state.get('book_size') else 0
        traded_size = float(state.get('traded_size', ['0'])[0]) if state.get('traded_size') else 0
        
        # Convert timestamp from nanoseconds to milliseconds
        create_time_ns = int(order.get('metadata', {}).get('create_time', '0'))
        timestamp_ms = create_time_ns // 1_000_000  # Convert nanoseconds to milliseconds
        
        # Convert to ISO8601 datetime
        datetime_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Build CCXT order
        ccxt_order = {
            'id': order.get('order_id', ''),
            'clientOrderId': str(order.get('metadata', {}).get('client_order_id', '')),
            'datetime': datetime_str,
            'timestamp': timestamp_ms,
            'lastTradeTimestamp': None,
            'status': status_mapping.get(state.get('status', 'OPEN'), 'open'),
            'symbol': leg.get('instrument', 'ETH/USDT').replace('_', '/'),
            'type': 'market' if order.get('is_market', False) else 'limit',
            'timeInForce': map_time_in_force(order.get('time_in_force', 'GOOD_TILL_TIME')),
            'side': side,
            'price': float(leg.get('limit_price', 0)),
            'average': float(state.get('avg_fill_price', ['0'])[0]) if state.get('avg_fill_price') else 0,
            'amount': book_size,
            'filled': traded_size,
            'remaining': book_size - traded_size,
            'cost': traded_size * float(leg.get('limit_price', 0)),
            'trades': [],
            'fee': {},
            'reduceOnly': order.get('reduce_only', False),
            'postOnly': order.get('post_only', False),
            'info': order  # Store original order as info
        }
        
        return ccxt_order
        
    except Exception as e:
        logger.error(f"Error converting GRVT order to CCXT: {e}", exc_info=True)
        return {
            'id': order.get('order_id', ''),
            'clientOrderId': str(order.get('metadata', {}).get('client_order_id', '')),
            'status': 'unknown',
            'symbol': 'ETH/USDT',
            'side': 'buy',
            'price': 0,
            'amount': 0,
            'filled': 0,
            'remaining': 0,
            'cost': 0,
            'info': order,
            'error': str(e)
        }


def map_time_in_force(time_in_force: str) -> str:
    """Map exchange time_in_force to CCXT format."""
    mapping = {
        # GRVT time_in_force values
        'GOOD_TILL_TIME': 'GTC',
        'IMMEDIATE_OR_CANCEL': 'IOC',
        'FILL_OR_KILL': 'FOK',
        'GOOD_TILL_CANCEL': 'GTC',
        # StandX time_in_force values
        'gtc': 'GTC',
        'ioc': 'IOC',
        'fok': 'FOK',
        'gtx': 'GTC'
    }
    return mapping.get(time_in_force, 'GTC')


def convert_standx_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    """Convert StandX order format to CCXT format."""
    try:
        # Map status
        status_mapping = {
            'new': 'open',
            'open': 'open',
            'filled': 'closed',
            'canceled': 'canceled',
            'expired': 'expired',
            'rejected': 'rejected'
        }
        
        # Map side
        side = order.get('side', 'buy')
        
        # Convert string numbers to floats
        price = float(order.get('price', '0'))
        amount = float(order.get('qty', '0'))
        filled = float(order.get('fill_qty', '0'))
        average_price = float(order.get('fill_avg_price', '0'))
        
        # Calculate remaining amount
        remaining = amount - filled
        
        # Convert symbol format from BTC-USD to BTC/USDT
        symbol = order.get('symbol', 'BTC-USD').replace('-', '/')
        
        # Convert timestamp from ISO8601 to Unix timestamp in milliseconds
        created_at = order.get('created_at', '')
        if created_at:
            # Parse ISO8601 format like "2025-08-11T03:35:25.559151Z"
            try:
                dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                timestamp_ms = int(dt.timestamp() * 1000)
                datetime_str = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            except:
                timestamp_ms = 0
                datetime_str = created_at
        else:
            timestamp_ms = 0
            datetime_str = ''
        
        # Build CCXT order
        ccxt_order = {
            'id': str(order.get('id', '')),
            'clientOrderId': str(order.get('cl_ord_id', '')),
            'datetime': datetime_str,
            'timestamp': timestamp_ms,
            'lastTradeTimestamp': None,
            'status': status_mapping.get(order.get('status', 'open'), 'open'),
            'symbol': symbol,
            'type': order.get('order_type', 'limit'),
            'timeInForce': map_time_in_force(order.get('time_in_force', 'gtc')),
            'side': side,
            'price': price,
            'average': average_price,
            'amount': amount,
            'filled': filled,
            'remaining': remaining,
            'cost': filled * average_price,
            'trades': [],
            'fee': {},
            'reduceOnly': order.get('reduce_only', False),
            'postOnly': False,  # StandX doesn't have post_only field
            'info': order  # Store original order as info
        }
        
        return ccxt_order
        
    except Exception as e:
        logger.error(f"Error converting StandX order to CCXT: {e}", exc_info=True)
        return {
            'id': str(order.get('id', '')),
            'clientOrderId': str(order.get('cl_ord_id', '')),
            'status': 'unknown',
            'symbol': 'BTC/USDT',
            'side': 'buy',
            'price': 0,
            'amount': 0,
            'filled': 0,
            'remaining': 0,
            'cost': 0,
            'info': order,
            'error': str(e)
        }


def convert_nado_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Nado order format to CCXT format."""
    try:
        # Map order_type from appendix
        order_type_mapping = {
            'default': 'limit',
            'ioc': 'limit',
            'fok': 'limit',
            'post_only': 'limit'
        }
        
        # Map status
        status_mapping = {
            'open': 'open',
            'filled': 'closed',
            'canceled': 'canceled',
            'expired': 'expired',
            'rejected': 'rejected'
        }
        
        # Nado uses x18 precision (10^18)
        X18 = 10 ** 18
        
        # Parse price and amount from x18 format
        # Handle both camelCase (priceX18) and snake_case (price_x18) variations
        price_x18 = int(order.get('price_x18') or order.get('priceX18') or order.get('price') or '0')
        amount_raw = int(order.get('amount', '0'))
        unfilled_amount_raw = int(order.get('unfilled_amount') or order.get('unfilledAmount') or amount_raw)
        
        logger.debug(f"Parsing Nado order: price_x18={price_x18}, amount={amount_raw}, unfilled={unfilled_amount_raw}")
        
        price = price_x18 / X18
        amount = abs(amount_raw) / X18
        unfilled = abs(unfilled_amount_raw) / X18
        filled = amount - unfilled
        
        # Determine side from amount sign (positive = buy, negative = sell)
        side = 'buy' if amount_raw > 0 else 'sell'
        
        # Get order type from order_type field or appendix
        nado_order_type = order.get('order_type', 'default')
        ccxt_order_type = order_type_mapping.get(nado_order_type, 'limit')
        
        # Map time in force based on order type
        time_in_force_mapping = {
            'default': 'GTC',
            'ioc': 'IOC',
            'fok': 'FOK',
            'post_only': 'GTC'
        }
        time_in_force = time_in_force_mapping.get(nado_order_type, 'GTC')
        
        # Get timestamp from placed_at if available
        placed_at = order.get('placed_at', 0)
        timestamp_ms = placed_at * 1000 if placed_at else None
        datetime_str = datetime.fromtimestamp(placed_at).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if placed_at else None
        
        # Get product_id and map to symbol
        product_id = order.get('product_id', 0)
        product_symbol_map = {
            0: 'USDT0',
            1: 'KBTC',
            2: 'BTC-PERP',
            3: 'WETH',
            4: 'ETH-PERP',
            5: 'USDC',
            8: 'SOL-PERP',
            10: 'XRP-PERP',
            14: 'BNB-PERP',
        }
        symbol = product_symbol_map.get(product_id, f'PRODUCT_{product_id}')
        
        # Get order ID (digest is the primary identifier in Nado)
        order_id = order.get('digest', '')
        if not order_id:
            order_id = str(order.get('nonce', ''))
        
        # Build CCXT order
        ccxt_order = {
            'id': order_id,
            'clientOrderId': str(int(order.get('nonce', 0)) & ((1 << 20) - 1)) if order.get('nonce') else '',
            'datetime': datetime_str,
            'timestamp': timestamp_ms,
            'lastTradeTimestamp': None,
            'status': status_mapping.get(order.get('status', 'open'), 'open'),
            'symbol': symbol,
            'type': ccxt_order_type,
            'timeInForce': time_in_force,
            'side': side,
            'price': price,
            'average': price,  # Nado doesn't provide average fill price in order query
            'amount': amount,
            'filled': filled,
            'remaining': unfilled,
            'cost': filled * price,
            'trades': [],
            'fee': {},
            'reduceOnly': False,  # Would need to parse from appendix
            'postOnly': nado_order_type == 'post_only',
            'info': order  # Store original order as info
        }
        
        return ccxt_order
        
    except Exception as e:
        logger.error(f"Error converting Nado order to CCXT: {e}", exc_info=True)
        return {
            'id': order.get('digest', ''),
            'clientOrderId': str(order.get('nonce', '')),
            'status': 'unknown',
            'symbol': 'ETH-PERP',
            'side': 'buy',
            'price': 0,
            'amount': 0,
            'filled': 0,
            'remaining': 0,
            'cost': 0,
            'info': order,
            'error': str(e)
        }


def convert_extended_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Extended order format to CCXT format."""
    try:
        # Map status
        status_mapping = {
            'NEW': 'open',
            'PARTIALLY_FILLED': 'open',
            'FILLED': 'closed',
            'CANCELLED': 'canceled',
            'CANCELED': 'canceled',
            'REJECTED': 'rejected',
            'EXPIRED': 'expired',
            'UNTRIGGERED': 'open',
            'TRIGGERED': 'open',
        }
        
        # Get order data
        order_id = str(order.get('id', ''))
        external_id = str(order.get('externalId', ''))
        market = order.get('market', 'ETH-USD')
        side = order.get('side', 'BUY').lower()
        status = status_mapping.get(order.get('status', 'NEW'), 'open')
        
        # Parse amounts
        price = float(order.get('price', '0') or '0')
        qty = float(order.get('qty', '0') or '0')
        filled_qty = float(order.get('filledQty', '0') or '0')
        remaining = qty - filled_qty
        average_price = float(order.get('averagePrice', '0') or '0') or price
        
        # Get timestamps
        created_time = order.get('createdTime')
        updated_time = order.get('updatedTime')
        
        # Map order type
        order_type = order.get('type', 'LIMIT').lower()
        if order_type in ['limit', 'conditional']:
            ccxt_type = 'limit'
        elif order_type == 'market':
            ccxt_type = 'market'
        else:
            ccxt_type = 'limit'
        
        # Time in force
        time_in_force = order.get('timeInForce', 'GTT')
        if time_in_force == 'IOC':
            tif = 'IOC'
        elif time_in_force == 'GTT':
            tif = 'GTC'
        else:
            tif = 'GTC'
        
        ccxt_order = {
            'id': order_id,
            'clientOrderId': external_id,
            'datetime': datetime.fromtimestamp(created_time / 1000).isoformat() if created_time else None,
            'timestamp': created_time,
            'lastTradeTimestamp': updated_time,
            'status': status,
            'symbol': market,
            'type': ccxt_type,
            'timeInForce': tif,
            'side': side,
            'price': price,
            'average': average_price,
            'amount': qty,
            'filled': filled_qty,
            'remaining': remaining,
            'cost': filled_qty * average_price,
            'trades': [],
            'fee': {'cost': float(order.get('payedFee', '0') or '0')},
            'reduceOnly': order.get('reduceOnly', False),
            'postOnly': order.get('postOnly', False),
            'info': order
        }
        
        return ccxt_order
        
    except Exception as e:
        logger.error(f"Error converting Extended order to CCXT: {e}", exc_info=True)
        return {
            'id': str(order.get('id', '')),
            'clientOrderId': str(order.get('externalId', '')),
            'status': 'unknown',
            'symbol': order.get('market', 'ETH-USD'),
            'side': order.get('side', 'BUY').lower(),
            'price': 0,
            'amount': 0,
            'filled': 0,
            'remaining': 0,
            'cost': 0,
            'info': order,
            'error': str(e)
        }


def convert_unknown_to_ccxt(order: Dict[str, Any]) -> Dict[str, Any]:
    """Convert unknown order format to basic CCXT format."""
    logger.warning(f"Converting unknown order format: {order}")
    
    return {
        'id': str(order.get('id', order.get('order_id', order.get('client_order_id', '')))),
        'clientOrderId': str(order.get('client_order_id', order.get('clientOrderId', ''))),
        'status': 'unknown',
        'symbol': 'ETH/USDT',
        'side': 'buy',
        'price': float(order.get('price', 0)),
        'amount': float(order.get('amount', order.get('size', 0))),
        'filled': float(order.get('filled', order.get('traded_size', 0))),
        'remaining': 0,
        'cost': 0,
        'info': order
    }


def normalize_orders_list(orders: Union[List[Dict], Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize a list of orders or a dictionary of orders to CCXT format.
    
    Args:
        orders: Either a list of orders or a dict with market_id -> orders mapping
        
    Returns:
        List of CCXT standardized orders
    """
    if isinstance(orders, dict):
        # Handle dict format (market_id -> orders list)
        normalized = []
        for market_orders in orders.values():
            if isinstance(market_orders, list):
                for order in market_orders:
                    normalized.append(normalize_order_to_ccxt(order))
        return normalized
    elif isinstance(orders, list):
        # Handle list format
        return [normalize_order_to_ccxt(order) for order in orders]
    else:
        logger.warning(f"Unknown orders format: {type(orders)}")
        return []