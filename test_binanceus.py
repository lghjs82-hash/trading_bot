import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

def test_binanceus():
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    
    print("--- Testing Binance.US (binanceus) ---")
    try:
        ex = ccxt.binanceus({
            'apiKey': key,
            'secret': secret,
            'enableRateLimit': True,
        })
        balance = ex.fetch_balance()
        print("Success! Binance.US balance fetched.")
    except Exception as e:
        print(f"Failed Binance.US: {e}")

if __name__ == "__main__":
    test_binanceus()
