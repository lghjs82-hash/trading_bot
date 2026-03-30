import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

def diagnostic():
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    
    # Try Live Futures (binanceusdm)
    print("--- Testing Live Futures (binanceusdm) ---")
    try:
        ex = ccxt.binanceusdm({
            'apiKey': key,
            'secret': secret,
            'enableRateLimit': True,
        })
        balance = ex.fetch_balance()
        print("Success! Live Futures balance fetched.")
    except Exception as e:
        print(f"Failed Live Futures: {e}")

    # Try Live Spot (binance)
    print("\n--- Testing Live Spot (binance) ---")
    try:
        ex_spot = ccxt.binance({
            'apiKey': key,
            'secret': secret,
            'enableRateLimit': True,
        })
        balance = ex_spot.fetch_balance()
        print("Success! Live Spot balance fetched.")
    except Exception as e:
        print(f"Failed Live Spot: {e}")

    # Try Testnet (usually has different hosts)
    print("\n--- Testing Testnet ---")
    try:
        ex_test = ccxt.binanceusdm({
            'apiKey': key,
            'secret': secret,
            'enableRateLimit': True,
        })
        ex_test.set_sandbox_mode(True)
        balance = ex_test.fetch_balance()
        print("Success! Testnet balance fetched.")
    except Exception as e:
        print(f"Failed Testnet: {e}")

if __name__ == "__main__":
    diagnostic()
