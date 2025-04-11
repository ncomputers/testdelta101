import os
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Instead of starting a tunnel programmatically, if you already have a domain, just use it:
NGROK_DOMAIN = os.getenv("NGROK_DOMAIN", "https://octopus-absolute-frequently.ngrok-free.app")

# Initially define DELTA_API_URLS based on the NGROK_DOMAIN (for legacy use)
DELTA_API_URLS = {
    'public': NGROK_DOMAIN + "/public",
    'private': NGROK_DOMAIN + "/private",
}

# Override DELTA_API_URLS with environment variables if provided
DELTA_API_URLS = {
    'public': os.getenv('DELTA_PUBLIC_URL', 'https://api.india.delta.exchange'),
    'private': os.getenv('DELTA_PRIVATE_URL', 'https://api.india.delta.exchange'),
}

# Fixed offset for trading logic
FIXED_OFFSET = int(os.getenv('FIXED_OFFSET', 100))
MISSING_PRICE_OFFSET = int(os.getenv('MISSING_PRICE_OFFSET', 100))


# Trading parameters
DEFAULT_ORDER_TYPE = 'limit'
TRAILING_STOP_PERCENT = 2.0  # 2% trailing stop
BASKET_ORDER_ENABLED = True

# Logging configuration
LOG_FILE = os.getenv('LOG_FILE', 'trading.log')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))

# Market data caching TTL (in seconds)
MARKET_CACHE_TTL = int(os.getenv('MARKET_CACHE_TTL', '300'))

# Database configuration (if needed)
DATABASE_URI = os.getenv('DATABASE_URI', 'sqlite:///trading.db')

# Profit trailing configuration
PROFIT_TRAILING_CONFIG = {
    "start_trailing_profit_pct": 0.005,  # trailing starts at 0.5% profit
    "levels": [
         {"min_profit_pct": 0.005, "trailing_stop_offset": 0.001, "book_fraction": 1.0},   # 0.5%-1%: stop = entry*(1+0.001)
         {"min_profit_pct": 0.01,  "trailing_stop_offset": 0.006, "book_fraction": 1.0},    # 1%-1.5%: stop = entry*(1+0.006)
         {"min_profit_pct": 0.015, "trailing_stop_offset": 0.012, "book_fraction": 1.0},    # 1.5%-2%: stop = entry*(1+0.012)
         {"min_profit_pct": 0.02,  "trailing_stop_offset": None, "book_fraction": 0.9}       # ≥2%: partial booking mode; new stop = entry*(1+profit_pct*0.9)
    ],
    "fixed_stop_loss_pct": 0.005,  # fixed stop loss at 0.5% adverse movement
    "trailing_unit": "percent"
}

# Multi-account configuration for managing multiple trading accounts
ACCOUNTS = {
    "MAIN": {
        "API_KEY": os.getenv("DELTA_API_KEY", "sUABSFPLpe5QNVJuKsOL6O0r5TiUoP"),
        "API_SECRET": os.getenv("DELTA_API_SECRET", "Q6Fo1NcOtNIxJZ9IPRUxROcSZ4vQdI31hDVPaoOvJnYfPt5wQLaNb6WMnNOy"),
        "REDIS_KEY": os.getenv("REDIS_KEY_MAIN", "signal_MAIN")
    },
    "V1": {
        "API_KEY": os.getenv("DELTA_API_KEY_V1", "woi6K2SqYM4pxucKKSyiWHC4otjhCG"),
        "API_SECRET": os.getenv("DELTA_API_SECRET_V1", "SbQPy3H8WArxN5SWguou3hgp9y1preJRgWkaEjTcwEgADLLqe55UlGBhBWS1"),
        "REDIS_KEY": os.getenv("REDIS_KEY_V1", "signal_V1")
    },
    "V2": {
        "API_KEY": os.getenv("DELTA_API_KEY_V2", "fGQsRZrluE94QnGNQen8z90pPW1I5s"),
        "API_SECRET": os.getenv("DELTA_API_SECRET_V2", "FXdGYpi7uUqez7NslLUao4QsIiTWIdcrnjv2AHrJbCZt4WBpgBc5EDXKC01w"),
        "REDIS_KEY": os.getenv("REDIS_KEY_V2", "signal_V2")
    }
}

# For backward compatibility, assign the MAIN account credentials to global variables.
API_KEY = ACCOUNTS["MAIN"]["API_KEY"]
API_SECRET = ACCOUNTS["MAIN"]["API_SECRET"]
REDIS_KEY = ACCOUNTS["MAIN"]["REDIS_KEY"]
