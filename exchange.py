import time
import ccxt
import config
import logging

logger = logging.getLogger(__name__)

class DeltaExchangeClient:
    def __init__(self, account="MAIN"):
        """
        Initializes the DeltaExchangeClient with account-specific API credentials.
        The 'account' parameter should be one of the keys in config.ACCOUNTS (e.g., "MAIN", "V1", "V2").
        """
        self.account = account
        account_config = config.ACCOUNTS.get(account)
        if not account_config:
            raise ValueError(f"Account '{account}' not found in configuration.")

        api_key = account_config.get("API_KEY")
        api_secret = account_config.get("API_SECRET")
        
        try:
            self.exchange = ccxt.delta({
                'apiKey': api_key,
                'secret': api_secret,
                'urls': {
                    'api': {
                        'public': config.DELTA_API_URLS['public'],
                        'private': config.DELTA_API_URLS['private'],
                    }
                },
                'enableRateLimit': True,
                'adjustForTimeDifference': True,
            })
            logger.debug("DeltaExchangeClient initialized successfully for account %s.", account)
        except Exception as e:
            logger.error("Error initializing DeltaExchangeClient for account %s: %s", account, e)
            raise

        self._market_cache = None
        self._market_cache_time = 0

    def load_markets(self, reload=False):
        current_time = time.time()
        if not reload and self._market_cache and (current_time - self._market_cache_time < config.MARKET_CACHE_TTL):
            logger.debug("Returning cached market data.")
            return self._market_cache
        try:
            markets = self.exchange.load_markets(reload)
            self._market_cache = markets
            self._market_cache_time = current_time
            logger.debug("Markets loaded: %s", list(markets.keys()))
            return markets
        except Exception as e:
            logger.error("Error loading markets: %s", e)
            raise

    def fetch_balance(self):
        try:
            balance = self.exchange.fetch_balance()
            logger.debug("Balance fetched: %s", balance)
            return balance
        except Exception as e:
            logger.error("Error fetching balance: %s", e)
            raise

    def create_limit_order(self, symbol, side, amount, price, params=None):
        try:
            order = self.exchange.create_order(symbol, 'limit', side, amount, price, params or {})
            logger.debug("Limit order created: %s", order)
            return order
        except Exception as e:
            logger.error("Error creating limit order: %s", e)
            raise

    def cancel_order(self, order_id, symbol, params=None):
        try:
            result = self.exchange.cancel_order(order_id, symbol, params or {})
            logger.debug("Order canceled: %s", result)
            return result
        except Exception as e:
            logger.error("Error canceling order: %s", e)
            raise
    # In exchange.py, inside DeltaExchangeClient:
    def create_order(self, symbol, order_type, side, amount, price=None, params=None):
        try:
            return self.exchange.create_order(symbol, order_type, side, amount, price, params or {})
        except Exception as e:
            logger.error("Error creating %s order: %s", order_type, e)
            raise
        

    def modify_bracket_order(self, order_id, product_id, product_symbol, bracket_params):
        request_body = {
            "id": order_id,
            "product_id": product_id,
            "product_symbol": product_symbol,
        }
        request_body.update(bracket_params)
        try:
            if hasattr(self.exchange, 'privatePutOrdersBracket'):
                order = self.exchange.privatePutOrdersBracket(request_body)
            else:
                order = self.exchange.request('orders/bracket', 'PUT', request_body)
            logger.debug("Modified bracket order on exchange: %s", order)
            return order
        except Exception as e:
            logger.error("Error modifying bracket order: %s", e)
            raise

    def fetch_positions(self):
        try:
            if hasattr(self.exchange, 'fetch_positions'):
                positions = self.exchange.fetch_positions()
                logger.debug("Positions fetched using fetch_positions: %s", positions)
                return positions
            else:
                positions = self.exchange.request('positions', 'GET', {})
                logger.debug("Positions fetched using direct request: %s", positions)
                return positions
        except Exception as e:
            logger.error("Error fetching positions: %s", e)
            raise

if __name__ == '__main__':
    # Example usage for different accounts:
    for account in ["MAIN", "V1", "V2"]:
        print(f"--- Testing account: {account} ---")
        client = DeltaExchangeClient(account=account)
        try:
            markets = client.load_markets()
            print("Markets loaded successfully:", list(markets.keys()))
        except Exception as e:
            print("Error loading markets:", e)
        try:
            balance = client.fetch_balance()
            print("Fetched balance:", balance)
        except Exception as e:
            print("Error fetching balance:", e)
        try:
            positions = client.fetch_positions()
            print("Fetched positions:", positions)
        except Exception as e:
            print("Error fetching positions:", e)
