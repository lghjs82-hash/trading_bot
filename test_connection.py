import os
from execution_engine import ExecutionEngine
from dotenv import load_dotenv
import logging

# Load .env
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestConnection")

def test_account():
    # 1. Check if .env is populated
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    if not api_key or not api_secret:
        logger.error("API Key or Secret missing in .env file! Please copy .env.example to .env and fill it.")
        return

    # 2. Initialize Engine
    try:
        engine = ExecutionEngine()
        logger.info(f"Targeting: {engine.mode} mode")
        
        # 3. Check Auth
        if engine.check_auth():
            logger.info("Successfully connected to Binance!")
            
            # 4. Fetch Ticker
            price = engine.get_market_price()
            if price:
                logger.info(f"Current ETH Price: {price} USDT")
            
            logger.info("Everything looks good! You are ready to run live_bot.py")
        else:
            logger.error("Authentication failed. Please verify your API Key and Secret.")
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    test_account()
