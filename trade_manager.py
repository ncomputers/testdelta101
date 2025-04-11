import time
import logging
import uuid
import redis
from exchange import DeltaExchangeClient
from order_manager import OrderManager
import config

logger = logging.getLogger(__name__)
TOLERANCE = 1e-6  # Tolerance to treat near-zero sizes as zero

class TradeManager:
    def __init__(self):
        """Initialize TradeManager with the DeltaExchangeClient and OrderManager."""
        self.client = DeltaExchangeClient()
        self.order_manager = OrderManager()
        self.highest_price = None

    def get_current_price(self, product_symbol):
        """
        Retrieve the current price for the given product using the exchange ticker.
        Returns:
            float: The latest price.
        """
        try:
            ticker = self.client.exchange.fetch_ticker(product_symbol)
            price = float(ticker.get('last'))
            return price
        except Exception as e:
            logger.error("Error fetching current price for %s: %s", product_symbol, e)
            raise

    def monitor_trailing_stop(self, bracket_order_id, product_symbol, trailing_stop_percent, update_interval=10):
        """
        Monitor and update the trailing stop based on the highest price reached.
        Args:
            bracket_order_id (str): The identifier for the bracket order to modify.
            product_symbol (str): The symbol to monitor (e.g., "BTCUSD").
            trailing_stop_percent (float): The percentage for setting the new stop loss.
            update_interval (int): How frequently (in seconds) to update the trailing stop.
        """
        logger.info("Starting trailing stop monitoring for %s", product_symbol)
        try:
            self.highest_price = self.get_current_price(product_symbol)
            logger.info("Initial highest price: %s", self.highest_price)
        except Exception as e:
            logger.error("Could not fetch initial price: %s", e)
            return

        while True:
            try:
                current_price = self.get_current_price(product_symbol)
            except Exception as e:
                logger.error("Error fetching price: %s", e)
                time.sleep(update_interval)
                continue

            if current_price > self.highest_price:
                self.highest_price = current_price
                logger.info("New highest price reached: %s", self.highest_price)

            new_stop_loss = self.highest_price * (1 - trailing_stop_percent / 100.0)
            logger.info("Current price: %.2f, New stop loss calculated: %.2f", current_price, new_stop_loss)
            stop_loss_order = {
                "order_type": "limit_order",
                "stop_price": f"{round(new_stop_loss, 2)}",
                "limit_price": f"{round(new_stop_loss * 0.99, 2)}"
            }
            try:
                modified_order = self.order_manager.modify_bracket_order(bracket_order_id, stop_loss_order)
                logger.info("Bracket order modified: %s", modified_order)
            except Exception as e:
                logger.error("Error modifying bracket order: %s", e)

            time.sleep(update_interval)

    def place_market_order(self, symbol, side, amount, params=None, force=False):
        """
        Place a market order for the given symbol and side.
        If force is False, the method checks for open positions and pending orders
        and will skip order placement if one exists.
        When force=True, these checks are bypassed to ensure an immediate close.
        """
        side_lower = side.lower()

        if not force:
            # 1. Confirm no open positions exist for the given side via API.
            try:
                positions = self.client.fetch_positions()
                for pos in positions:
                    pos_symbol = (pos.get('info', {}).get('product_symbol') or pos.get('symbol') or '')
                    if symbol not in pos_symbol:
                        continue
                    try:
                        size = float(pos.get('size') or pos.get('contracts') or 0)
                    except Exception:
                        size = 0.0
                    if side_lower == "buy" and size > 0:
                        logging.info("An open buy position exists for %s. Skipping market order.", symbol)
                        return None
                    if side_lower == "sell" and size < 0:
                        logging.info("An open sell position exists for %s. Skipping market order.", symbol)
                        return None
            except Exception as e:
                logging.error("Error fetching positions: %s", e)

            # 2. Confirm no pending orders exist for the same side via API.
            try:
                open_orders = self.client.exchange.fetch_open_orders(symbol)
                if open_orders:
                    for order in open_orders:
                        if order.get('side', '').lower() == side_lower:
                            logging.info("A pending %s order exists for %s. Skipping market order.", side, symbol)
                            return None
                else:
                    logging.info("No pending orders found for %s.", symbol)
            except Exception as e:
                logging.error("Error fetching open orders: %s", e)

            # 3. Clean up stale local orders.
            current_time = int(time.time() * 1000)
            stale_order_ids = [
                oid for oid, order in self.order_manager.orders.items()
                if current_time - order.get('timestamp', 0) > 60000
            ]
            for oid in stale_order_ids:
                del self.order_manager.orders[oid]
    
            # 4. Check local cache for any pending orders of the same side.
            for order in self.order_manager.orders.values():
                if order.get('side', '').lower() == side_lower and order.get('status') in ['open', 'pending']:
                    logging.info("Local pending %s order exists for %s. Skipping new order.", side, symbol)
                    return None

        # 5. Place the market order regardless if force==True.
        try:
            order = self.client.exchange.create_order(symbol, 'market', side, amount, None, params or {})
            order_id = order.get('id', str(uuid.uuid4()))
            order_info = {
                'id': order_id,
                'symbol': symbol,
                'side': side,
                'amount': amount,
                'params': params or {},
                'status': order.get('status', 'open'),
                'timestamp': order.get('timestamp', int(time.time() * 1000))
            }
            self.order_manager.orders[order_id] = order_info
            self.order_manager._store_order(order_info)
    
            time.sleep(1)  # Brief delay for processing.
    
            # Optionally, verify open position via API.
            positions_after = self.client.fetch_positions()
            for pos in positions_after:
                pos_symbol = (pos.get('info', {}).get('product_symbol') or pos.get('symbol') or '')
                if symbol not in pos_symbol:
                    continue
                try:
                    size = float(pos.get('size') or pos.get('contracts') or 0)
                except Exception:
                    size = 0.0
                if (side_lower == "buy" and size > 0) or (side_lower == "sell" and size < 0):
                    logging.info("Market order verified for %s.", symbol)
                    break
    
            logging.info("Market order placed: %s", order_info)
            return order_info
        except Exception as e:
            logging.error("Error placing market order for %s: %s", symbol, e)
            raise


if __name__ == '__main__':
    tm = TradeManager()
    logger.info("Testing market order placement...")
    try:
        market_order = tm.place_market_order("BTCUSD", "buy", 1, params={"time_in_force": "ioc"})
        logger.info("Market order placed: %s", market_order)
    except Exception as e:
        logger.error("Failed to place market order: %s", e)
