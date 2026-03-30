import ccxt
import os
import time
from dotenv import load_dotenv

load_dotenv()

def deep_diagnostic():
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    
    # Try multiple classes
    exchanges = {
        'Global Spot': ccxt.binance,
        'Global Futures (USD-M)': ccxt.binanceusdm,
        'Global Futures (Coin-M)': ccxt.binancecoinm,
        'Binance US': ccxt.binanceus,
        'Binance TR': ccxt.binancetr,
    }

    for name, ex_class in exchanges.items():
        print(f"--- Testing {name} ---")
        try:
            ex = ex_class({
                'apiKey': key,
                'secret': secret,
                'enableRateLimit': True,
                'options': {'adjustForTimeDifference': True}
            })
            # Try both Normal and Sandbox (if applicable)
            for sandbox in [False, True]:
                try:
                    mode = "Sandbox" if sandbox else "Production"
                    if sandbox:
                        try:
                            ex.set_sandbox_mode(True)
                        except:
                            continue # skip if not supported
                    
                    balance = ex.fetch_balance()
                    print(f"[{mode}] Success! {name} balance fetched.")
                    return # Exit on first success
                except Exception as e:
                    if "Invalid Api-Key" in str(e) or "code\":-2008" in str(e):
                        print(f"[{mode}] Failed: Invalid API Key")
                    elif "sandbox mode is not supported" in str(e):
                         print(f"[{mode}] Failed: Sandbox not supported")
                    else:
                        print(f"[{mode}] Error: {e}")
        except Exception as e:
             print(f"Initialization Error for {name}: {e}")

if __name__ == "__main__":
    deep_diagnostic()
