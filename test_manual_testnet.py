import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

def test_manual_testnet():
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    
    print("--- Testing Manual Testnet URL ---")
    try:
        # Manually set the testnet URL
        ex = ccxt.binanceusdm({
            'apiKey': key,
            'secret': secret,
            'enableRateLimit': True,
            'urls': {
                'api': {
                    'public': 'https://testnet.binancefuture.com/fapi/v1',
                    'private': 'https://testnet.binancefuture.com/fapi/v1',
                }
            }
        })
        balance = ex.fetch_balance()
        print("Success! Manually configured Testnet balance fetched.")
    except Exception as e:
        print(f"Failed Manual Testnet: {e}")

if __name__ == "__main__":
    test_manual_testnet()
