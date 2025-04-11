import json
import threading
import time
import websocket

# Global variables to store the latest BTC/USDT price and last update timestamp
current_price = None
last_update_time = 0
ws_app = None  # Will hold the active WebSocketApp instance

def on_message(ws, message):
    global current_price, last_update_time
    try:
        data = json.loads(message)
        # Ensure the necessary keys exist
        if "p" not in data or "q" not in data or "m" not in data:
            return
        trade_data = {
            "timestamp": time.time(),
            "price": float(data["p"]),
            "buy_qty": float(data["q"]) if not data["m"] else 0,
            "sell_qty": float(data["q"]) if data["m"] else 0
        }
        current_price = trade_data["price"]
        last_update_time = time.time()  # Update the timestamp with every message
    except Exception as e:
        print("Error processing message:", e)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)

def on_open(ws):
    print("WebSocket connection opened")
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": ["btcusdt@aggTrade"],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

def start_websocket():
    global ws_app
    ws_app = websocket.WebSocketApp(
        "wss://fstream.binance.com/ws",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.on_open = on_open
    ws_app.run_forever()

def monitor_websocket():
    """
    Monitor the live price update. If no update occurs in the last 3 seconds,
    close the existing WebSocket to trigger a reconnect.
    """
    while True:
        time.sleep(2)
        if time.time() - last_update_time > 5:
            print("No price update in last 3 seconds. Reconnecting websocket...")
            try:
                ws_app.close()
            except Exception as e:
                print("Error closing websocket:", e)
            # The ws_app.run_forever() loop will exit; restart the websocket in a new thread
            threading.Thread(target=start_websocket, daemon=True).start()

def run_in_thread():
    """
    Start the Binance WebSocket and the monitor in separate daemon threads.
    """
    threading.Thread(target=start_websocket, daemon=True).start()
    threading.Thread(target=monitor_websocket, daemon=True).start()
    return

if __name__ == "__main__":
    run_in_thread()
    while True:
        if current_price is not None:
            print(f"Latest BTC/USDT price: {current_price}")
        time.sleep(2)
