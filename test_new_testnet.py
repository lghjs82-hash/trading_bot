import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

def test_new_testnet_style():
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    
    print("--- Testing New Testnet Style ('test': True) ---")
    try:
        # Use the standard way for the new Binance Testnet
        ex = ccxt.binanceusdm({
            'apiKey': key,
            'secret': secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'test': True  # This should point to testnet.binancefuture.com
            }
        })
        balance = ex.fetch_balance()
        print("Success! New Style Testnet balance fetched.")
    except Exception as e:
        print(f"Failed New Style Testnet: {e}")

if __name__ == "__main__":
    test_new_testnet_style()
