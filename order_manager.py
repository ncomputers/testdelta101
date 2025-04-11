import logging
import time
import json
import redis
from exchange import DeltaExchangeClient
import config

logger = logging.getLogger(__name__)

class OrderManager:
    def __init__(self):
        """Initialize the OrderManager with an exchange client, local order storage, and Redis client."""
        self.client = DeltaExchangeClient()
        self.orders = {}  # Local cache for orders
        self.redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)

    def _store_order(self, order_info):
        """Store or update the order info in Redis."""
        key = f"order:{order_info['id']}"
        try:
            self.redis_client.set(key, json.dumps(order_info))
        except Exception as e:
            logger.error("Error storing order in Redis: %s", e)

    def is_order_open(self, symbol, side):
        """
        Check if an order is open for the given symbol and side.
        First, attempt to check via the API; if that fails, check the local cache.
        """
        try:
            for order in self.client.exchange.fetch_open_orders(symbol):
                if order.get('side', '').lower() == side.lower() and order.get('status', '').lower() == 'open':
                    return True
        except Exception as e:
            logger.error("Error checking open orders via API: %s", e)

        # Fallback: check the local cache.
        for order in self.orders.values():
            if (order.get('symbol') == symbol and
                order.get('side', '').lower() == side.lower() and
                order.get('status', '').lower() == 'open'):
                return True
        return False

    def has_open_position(self, symbol, side):
        """
        Returns True if an actual position is open for the given symbol and side.
        For 'buy' positions, size > 0; for 'sell' positions, size < 0.
        """
        try:
            for pos in self.client.fetch_positions():
                pos_symbol = pos.get('info', {}).get('product_symbol') or pos.get('symbol') or ''
                if symbol not in pos_symbol:
                    continue
                try:
                    size = float(pos.get('size') or pos.get('contracts') or 0)
                except Exception:
                    size = 0.0

                if side.lower() == "buy" and size > 0:
                    return True
                if side.lower() == "sell" and size < 0:
                    return True
        except Exception as e:
            logger.error("Error checking open positions via API: %s", e)
        return False

    def place_order(self, symbol, side, amount, price, params=None):
        """
        Place a new limit order using the exchange client.
        The order information is cached locally and stored in Redis.
        """
        try:
            order = self.client.create_limit_order(symbol, side, amount, price, params)
            order_id = order.get('id') or int(time.time() * 1000)
            order_info = {
                'id': order_id,
                'symbol': symbol,
                'side': side,
                'amount': amount,
                'price': price,
                'params': params or {},
                'status': order.get('status', 'open'),
                'timestamp': order.get('timestamp', int(time.time() * 1000))
            }
            self.orders[order_id] = order_info
            self._store_order(order_info)
            logger.debug("Placed order: %s", order_info)
            return order_info
        except Exception as e:
            logger.error("Error placing order for %s: %s", symbol, e)
            raise

    def attach_bracket_to_order(self, order_id, product_id, product_symbol, bracket_params):
        """
        Attach or update bracket parameters (such as SL and TP) to an existing order.
        If no local record exists, a new one is created using the exchange response.
        """
        try:
            exchange_order = self.client.modify_bracket_order(order_id, product_id, product_symbol, bracket_params)
            if order_id in self.orders:
                # Update existing order details.
                self.orders[order_id]['params'].update(bracket_params)
                self.orders[order_id]['status'] = exchange_order.get('state', self.orders[order_id]['status'])
                updated_order = self.orders[order_id]
            else:
                # Create a new order record if missing.
                updated_order = {
                    'id': order_id,
                    'product_id': product_id,
                    'product_symbol': product_symbol,
                    'params': bracket_params,
                    'status': exchange_order.get('state', 'open'),
                    'timestamp': exchange_order.get('created_at', int(time.time() * 1000000))
                }
                self.orders[order_id] = updated_order

            self._store_order(updated_order)
            logger.debug("Bracket attached to order %s: %s", order_id, updated_order)
            return updated_order
        except Exception as e:
            logger.error("Error attaching bracket to order %s: %s", order_id, e)
            raise

    def modify_bracket_order(self, order_id, new_bracket_params):
        """
        Modify the bracket parameters of an existing order.
        """
        if order_id not in self.orders:
            raise ValueError("Bracket order ID not found.")
        self.orders[order_id]['params'].update(new_bracket_params)
        self._store_order(self.orders[order_id])
        logger.debug("Modified bracket order %s locally: %s", order_id, self.orders[order_id])
        return self.orders[order_id]

    def cancel_order(self, order_id):
        """
        Cancel the order identified by order_id.
        The local cache is updated to mark the order as canceled.
        """
        if order_id not in self.orders:
            raise ValueError("Order ID not found.")
        order = self.orders[order_id]
        symbol = order.get('symbol') or order.get('product_symbol')
        try:
            result = self.client.cancel_order(order_id, symbol)
            order['status'] = 'canceled'
            self._store_order(order)
            logger.debug("Canceled order %s: %s", order_id, result)
            return result
        except Exception as e:
            logger.error("Error canceling order %s: %s", order_id, e)
            raise

# Example usage for testing purposes:
if __name__ == '__main__':
    om = OrderManager()
    try:
        limit_order = om.place_order("BTCUSD", "buy", 1, 45000)
        print("Limit order placed:", limit_order)
    except Exception as e:
        print("Failed to place limit order:", e)
        exit(1)

    bracket_params = {
        "bracket_stop_loss_limit_price": "50000",
        "bracket_stop_loss_price": "50000",
        "bracket_take_profit_limit_price": "55000",
        "bracket_take_profit_price": "55000",
        "bracket_stop_trigger_method": "last_traded_price"
    }
    try:
        updated_order = om.attach_bracket_to_order(
            order_id=limit_order['id'],
            product_id=27,
            product_symbol="BTCUSD",
            bracket_params=bracket_params
        )
        print("Bracket attached, updated order:", updated_order)
    except Exception as e:
        print("Failed to attach bracket to order:", e)
