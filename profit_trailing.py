import time
import logging
import threading
import json
import redis
from exchange import DeltaExchangeClient
import config
from trade_manager import TradeManager
import binance_ws  # For live price updates via WebSocket

logger = logging.getLogger(__name__)

class ProfitTrailing:
    def __init__(self, check_interval=1):
        self.client = DeltaExchangeClient()
        self.trade_manager = TradeManager()
        self.check_interval = check_interval
        self.position_trailing_stop = {}  # Cache: order_id -> trailing stop price
        self.last_had_positions = True
        self.last_position_fetch_time = 0
        self.position_fetch_interval = 5
        self.cached_positions = []
        self.last_display = {}
        self.position_max_profit = {}
        self.take_profit_signal_received = False
        self.redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)

    def fetch_open_positions(self):
        try:
            positions = self.client.fetch_positions()
            open_positions = []
            for pos in positions:
                size = pos.get('size') or pos.get('contracts') or 0
                try:
                    size = float(size)
                except Exception:
                    size = 0.0
                if size != 0:
                    pos_symbol = (pos.get('info', {}).get('product_symbol') or pos.get('symbol'))
                    if pos_symbol and "BTCUSD" in pos_symbol:
                        open_positions.append(pos)
            return open_positions
        except Exception as e:
            logger.error("Error fetching open positions: %s", e)
            return []

    def compute_profit_pct(self, pos, live_price):
        entry = pos.get('entryPrice') or pos.get('entry_price') or pos.get('info', {}).get('entry_price')
        try:
            entry = float(entry)
        except Exception:
            return None
        size = pos.get('size') or pos.get('contracts') or 0
        try:
            size = float(size)
        except Exception:
            size = 0.0
        if size > 0:
            return (live_price - entry) / entry
        else:
            return (entry - live_price) / entry

    def update_trailing_stop(self, pos, live_price):
        order_id = pos.get('id') or f"{pos.get('symbol')}_{pos.get('entryPrice')}"
        entry = pos.get('entryPrice') or pos.get('entry_price') or pos.get('info', {}).get('entry_price')
        try:
            entry = float(entry)
        except Exception:
            return None, None, None
        size = pos.get('size') or pos.get('contracts') or 0
        try:
            size = float(size)
        except Exception:
            size = 0.0
        current_profit = live_price - entry if size > 0 else entry - live_price

        prev_max = self.position_max_profit.get(order_id, 0)
        new_max_profit = max(prev_max, current_profit)
        self.position_max_profit[order_id] = new_max_profit

        # ✅ Lock_50: Only active after take profit signal
        if self.take_profit_signal_received and new_max_profit > 1000 and current_profit > 0:
            lock_sl = entry  # ✅ Lock to entry price (breakeven)
            stored_trailing = self.position_trailing_stop.get(order_id)
            if stored_trailing is not None:
                if size > 0:
                    new_trailing = max(stored_trailing, lock_sl)
                else:
                    new_trailing = min(stored_trailing, lock_sl)
            else:
                new_trailing = lock_sl
            self.position_trailing_stop[order_id] = new_trailing
            return new_trailing, new_max_profit / entry, "lock_50"

        default_sl = entry - 500 if size > 0 else entry + 500
        stored_trailing = self.position_trailing_stop.get(order_id)
        if stored_trailing is not None:
            new_trailing = max(stored_trailing, default_sl) if size > 0 else min(stored_trailing, default_sl)
        else:
            new_trailing = default_sl
        self.position_trailing_stop[order_id] = new_trailing
        return new_trailing, current_profit / entry, "fixed_stop"

    def compute_raw_profit(self, pos, live_price):
        entry = pos.get('entryPrice') or pos.get('entry_price') or pos.get('info', {}).get('entry_price')
        try:
            entry = float(entry)
        except Exception:
            return None
        size = pos.get('size') or pos.get('contracts') or 0
        try:
            size = float(size)
        except Exception:
            size = 0.0
        return (live_price - entry) * size if size > 0 else (entry - live_price) * abs(size)

    def book_profit(self, pos, live_price):
        order_id = pos.get('id') or f"{pos.get('symbol')}_{pos.get('entryPrice')}"
        size = pos.get('size') or pos.get('contracts') or 0
        try:
            size = float(size)
        except Exception:
            size = 0.0
        trailing_stop, profit_ratio, rule = self.update_trailing_stop(pos, live_price)
        if rule in ["dynamic", "fixed_stop", "lock_50"]:
            if size > 0 and live_price < trailing_stop:
                close_order = self.trade_manager.place_market_order("BTCUSD", "sell", size,
                                                                    params={"time_in_force": "ioc"},
                                                                    force=True)
                logger.info("Trailing stop triggered for long order %s. Closing position. Close order: %s", order_id, close_order)
                self.cached_positions = []  # 🧠 Force refresh
                return True
            elif size < 0 and live_price > trailing_stop:
                close_order = self.trade_manager.place_market_order("BTCUSD", "buy", abs(size),
                                                                    params={"time_in_force": "ioc"},
                                                                    force=True)
                logger.info("Trailing stop triggered for short order %s. Closing position. Close order: %s", order_id, close_order)
                self.cached_positions = []  # 🧠 Force refresh
                return True


        return False

    def check_take_profit_signal(self):
        if not self.cached_positions:
            self.take_profit_signal_received = False
            return
        try:
            data = self.redis_client.get("signal")
            if not data:
                return
            parsed = json.loads(data)
            signal_text = parsed.get("last_signal", {}).get("text", "").lower()
            if "take profit" in signal_text or "tp" in signal_text:
                if not self.take_profit_signal_received:
                    logger.info("Take profit signal detected. Activating TP-based trailing logic.")
                self.take_profit_signal_received = True
        except Exception as e:
            logger.error("Failed to check TP signal from Redis: %s", e)

    def track(self):
        thread = threading.Thread(target=binance_ws.run_in_thread, daemon=True)
        thread.start()

        wait_time = 0
        while binance_ws.current_price is None and wait_time < 30:
            logger.info("Waiting for live price update...")
            time.sleep(2)
            wait_time += 2

        if binance_ws.current_price is None:
            logger.warning("Live price not available. Exiting profit trailing tracker.")
            return

        while True:
            current_time = time.time()
            if current_time - self.last_position_fetch_time >= self.position_fetch_interval:
                self.cached_positions = self.fetch_open_positions()
                self.last_position_fetch_time = current_time
                if not self.cached_positions:
                    self.position_trailing_stop.clear()

            self.check_take_profit_signal()

            live_price = binance_ws.current_price
            if live_price is None:
                continue

            open_positions = self.cached_positions
            if not open_positions:
                if self.last_had_positions:
                    logger.info("No open positions. Profit trailing paused.")
                    self.last_had_positions = False
                self.position_trailing_stop.clear()
            else:
                if not self.last_had_positions:
                    logger.info("Open positions detected. Profit trailing resumed.")
                    self.last_had_positions = True

                for pos in open_positions:
                    order_id = pos.get('id') or f"{pos.get('symbol')}_{pos.get('entryPrice')}"
                    try:
                        size = float(pos.get('size') or pos.get('contracts') or 0)
                    except Exception:
                        size = 0.0
                    if size == 0:
                        continue

                    entry = pos.get('entryPrice') or pos.get('entry_price') or pos.get('info', {}).get('entry_price')
                    try:
                        entry_val = float(entry)
                    except Exception:
                        entry_val = None

                    profit_pct = self.compute_profit_pct(pos, live_price)
                    profit_display = profit_pct * 100 if profit_pct is not None else None
                    raw_profit = self.compute_raw_profit(pos, live_price)
                    profit_usd = raw_profit / 1000 if raw_profit is not None else None
                    trailing_stop, _, rule = self.update_trailing_stop(pos, live_price)
                    max_profit = self.position_max_profit.get(order_id, 0)

                    display = {
                        "entry": entry_val,
                        "live": live_price,
                        "profit": round(profit_display or 0, 2),
                        "usd": round(profit_usd or 0, 2),
                        "rule": rule,
                        "sl": round(trailing_stop or 0, 2),
                        "max": round(max_profit, 2)
                    }

                    if self.last_display.get(order_id) != display:
                        logger.info(
                            f"Order: {order_id} | Entry: {entry_val:.1f} | Live: {live_price:.1f} | "
                            f"PnL: {profit_display:.2f}% | USD: {profit_usd:.2f} | Rule: {rule} | SL: {trailing_stop:.1f} | Max: {max_profit:.1f}"
                        )
                        self.last_display[order_id] = display

                    if self.book_profit(pos, live_price):
                        logger.info(f"Profit booked for order {order_id}.")

            time.sleep(self.check_interval)

if __name__ == '__main__':
    pt = ProfitTrailing(check_interval=1)
    pt.track()
