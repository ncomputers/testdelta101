import time
import json
import redis
import logging
from order_manager import OrderManager
from trade_manager import TradeManager
from profit_trailing import ProfitTrailing
import config
import binance_ws  # For live price fallback

logger = logging.getLogger(__name__)

class SignalProcessor:
    def __init__(self):
        self.order_manager = OrderManager()
        self.trade_manager = TradeManager()
        self.redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        self.last_signal = None  
        self.last_executed_side = None
        self.profit_trailing_instance = ProfitTrailing(check_interval=1)
        self.last_signal_time = 0  
        self.debounce_threshold = 10  # seconds

    def normalize_signal(self, signal):
        try:
            text = signal.get("last_signal", {}).get("text", "")
            return text.strip().lower() if text else ""
        except Exception:
            return ""

    def fetch_signal(self, key="signal"):
        try:
            data = self.redis_client.get(key)
            if not data:
                return None
            return json.loads(data)
        except Exception as e:
            logger.error("Error fetching signal from Redis: %s", e)
            return None

    def cancel_conflicting_orders(self, symbol, new_side):
        try:
            orders = self.order_manager.client.exchange.fetch_open_orders(symbol)
            if orders:
                for order in orders:
                    if order.get('status', '').lower() != 'open':
                        continue
                    order_side = order.get('side', '').lower()
                    if new_side == "" or order_side != new_side.lower():
                        try:
                            self.order_manager.client.cancel_order(order['id'], symbol)
                            logger.info("Canceled conflicting order: %s", order['id'])
                        except Exception as e:
                            logger.error("Error canceling order %s: %s", order['id'], e)
        except Exception as e:
            logger.error("Error fetching open orders: %s", e)

    def cancel_same_side_orders(self, symbol, side):
        try:
            pending_orders = self.order_manager.client.exchange.fetch_open_orders(symbol)
            for order in pending_orders:
                if order.get('side', '').lower() == side.lower() and order.get('status', '').lower() == 'open':
                    try:
                        self.order_manager.client.cancel_order(order['id'], symbol)
                        logger.info("Canceled same-side order: %s", order['id'])
                    except Exception as e:
                        logger.error("Error canceling same-side order %s: %s", order['id'], e)
        except Exception as e:
            logger.error("Error fetching same-side pending orders: %s", e)

    def open_pending_order_exists(self, symbol, side):
        try:
            orders = self.order_manager.client.exchange.fetch_open_orders(symbol)
            for order in orders:
                if order.get('side', '').lower() == side.lower() and order.get('status', '').lower() == 'open':
                    return True
            return False
        except Exception as e:
            logger.error("Error checking for pending orders: %s", e)
            return False

    def signals_are_different(self, new_signal_norm, old_signal_norm):
        return new_signal_norm != old_signal_norm

    def process_signal(self, signal_data):
        if not signal_data:
            return None

        current_time = time.time()
        if current_time - self.last_signal_time < self.debounce_threshold:
            logger.debug("Signal debounce active. Skipping signal processing.")
            return None

        norm_text = self.normalize_signal(signal_data)
        if not norm_text:
            return None

        raw_price = signal_data.get("last_signal", {}).get("price")
        raw_supply = signal_data.get("supply_zone", {}).get("min")
        raw_demand = signal_data.get("demand_zone", {}).get("min")

        if raw_price is None or str(raw_price).strip() == "":
            live_price = binance_ws.current_price
            if live_price is None:
                logger.error("No valid price in signal and live price unavailable.")
                return None
            raw_price = live_price
            logger.info("Using live price as fallback: %.2f", raw_price)

        if "take profit" in norm_text or "tp" in norm_text:
            logger.info("Take profit signal detected. Activating TP-based trailing logic.")
            self.profit_trailing_instance.tp_triggered = True
            live_price = binance_ws.current_price
            if live_price is None:
                logger.error("Live price unavailable for TP signal processing.")
                return None
            try:
                positions = self.order_manager.client.fetch_positions()
                for pos in positions:
                    pos_symbol = pos.get('info', {}).get('product_symbol') or pos.get('symbol')
                    if pos_symbol and "BTCUSD" in pos_symbol:
                        entry = pos.get('entryPrice') or pos.get('entry_price') or pos.get('info', {}).get('entry_price')
                        try:
                            entry = float(entry)
                        except Exception:
                            continue
                        size = pos.get('size') or pos.get('contracts') or 0
                        try:
                            size = float(size)
                        except Exception:
                            size = 0.0
                        if size == 0:
                            continue
                        if size > 0:
                            profit_pct = (live_price - entry) / entry
                            if profit_pct < 0:
                                close_order = self.trade_manager.place_market_order(
                                    "BTCUSD", "sell", size,
                                    params={"time_in_force": "ioc"}, force=True
                                )
                                logger.info("TP signal: Force closing long position in loss. Close order: %s", close_order)
                        elif size < 0:
                            profit_pct = (entry - live_price) / entry
                            if profit_pct < 0:
                                close_order = self.trade_manager.place_market_order(
                                    "BTCUSD", "buy", abs(size),
                                    params={"time_in_force": "ioc"}, force=True
                                )
                                logger.info("TP signal: Force closing short position in loss. Close order: %s", close_order)
            except Exception as e:
                logger.error("Error processing TP signal: %s", e)
            self.last_signal_time = current_time
            return None

        if "short" in norm_text:
            new_side = "sell"
        elif "buy" in norm_text:
            new_side = "buy"
        else:
            new_side = None

        try:
            positions = self.order_manager.client.fetch_positions()
            for pos in positions:
                pos_symbol = pos.get('info', {}).get('product_symbol') or pos.get('symbol')
                if pos_symbol and "BTCUSD" in pos_symbol:
                    pos_size = float(pos.get('size') or pos.get('contracts') or 0)
                    if new_side == "buy" and pos_size < 0:
                        logger.info("Opposite signal: Forcing closure of short before buying.")
                        self.trade_manager.place_market_order(
                            "BTCUSD", "buy", abs(pos_size),
                            params={"time_in_force": "ioc"}, force=True
                        )
                        time.sleep(2)
                    elif new_side == "sell" and pos_size > 0:
                        logger.info("Opposite signal: Forcing closure of long before selling.")
                        self.trade_manager.place_market_order(
                            "BTCUSD", "sell", pos_size,
                            params={"time_in_force": "ioc"}, force=True
                        )
                        time.sleep(2)
        except Exception as e:
            logger.error("Error handling opposite positions: %s", e)

        self.cancel_conflicting_orders("BTCUSD", new_side)
        self.cancel_same_side_orders("BTCUSD", new_side)
        time.sleep(2)
        if new_side and self.open_pending_order_exists("BTCUSD", new_side):
            logger.info("A pending %s order exists for BTCUSD. Skipping new order.", new_side)
            self.last_signal_time = current_time
            return None

        if raw_supply is None or raw_demand is None:
            logger.error("Incomplete signal data (supply/demand missing): %s", signal_data)
            self.last_signal_time = current_time
            return None

        if new_side == "buy":
            entry_price = float(raw_price) - 50
            sl_price = float(raw_price) - 500
            tp_price = float(raw_price) + 3000
        elif new_side == "sell":
            entry_price = float(raw_price) + 50
            sl_price = float(raw_price) + 500
            tp_price = float(raw_price) - 3000
        else:
            logger.warning("Unable to determine side from signal: %s", norm_text)
            self.last_signal_time = current_time
            return None

        signal_text = signal_data.get("last_signal", {}).get("text", "")
        logger.info("Signal: %s | Entry: %.2f | SL: %.2f | TP: %.2f",
                    signal_text, entry_price, sl_price, tp_price)

        try:
            limit_order = self.order_manager.place_order("BTCUSD", new_side, 1, entry_price,
                                                          params={"time_in_force": "gtc"})
            logger.info("Limit order placed: %s", limit_order)
        except Exception as e:
            logger.error("Failed to place limit order: %s", e)
            self.last_signal_time = current_time
            return None

        bracket_params = {
            "bracket_stop_loss_limit_price": str(sl_price),
            "bracket_stop_loss_price": str(sl_price),
            "bracket_take_profit_limit_price": str(tp_price),
            "bracket_take_profit_price": str(tp_price),
            "bracket_stop_trigger_method": "last_traded_price"
        }
        try:
            updated_order = self.order_manager.attach_bracket_to_order(
                order_id=limit_order['id'],
                product_id=27,
                product_symbol="BTCUSD",
                bracket_params=bracket_params
            )
            logger.info("Bracket attached to order: %s", updated_order)
            self.last_signal_time = current_time
            self.last_signal = {"text": norm_text}
            return updated_order
        except Exception as e:
            logger.error("Failed to attach bracket: %s", e)
            self.last_signal_time = current_time
            return None

    def process_signals_loop(self, sleep_interval=5):
        logger.info("Starting signal processing loop...")
        while True:
            signal_data = self.fetch_signal()
            new_signal_norm = self.normalize_signal(signal_data) if signal_data else ""
            old_signal_norm = self.last_signal.get("text", "") if self.last_signal else ""
            if signal_data and self.signals_are_different(new_signal_norm, old_signal_norm):
                logger.info("New signal detected.")
                processed = self.process_signal(signal_data)
                if processed:
                    logger.info("Order processed successfully: %s", processed)
                else:
                    logger.info("Signal processing skipped or failed.")
            else:
                logger.debug("No new signal or signal unchanged.")
            time.sleep(sleep_interval)

if __name__ == '__main__':
    sp = SignalProcessor()
    sp.process_signals_loop()
