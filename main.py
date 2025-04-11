import threading
import logging
from logger import setup_logging
from profit_trailing import ProfitTrailing
from signal_processor import SignalProcessor

def profit_trailing_thread():
    pt = ProfitTrailing(check_interval=1)
    pt.track()

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Start profit trailing in a daemon thread.
    pt_thread = threading.Thread(target=profit_trailing_thread, daemon=True)
    pt_thread.start()
    
    # Create and run the signal processor in the main thread.
    sp = SignalProcessor()
    sp.process_signals_loop(sleep_interval=5)

if __name__ == '__main__':
    main()
