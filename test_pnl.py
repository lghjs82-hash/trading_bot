import ccxt
import os
from dotenv import load_dotenv

load_dotenv('.env', override=True)

try:
    exchange = ccxt.binanceusdm({
        'apiKey': os.getenv('TESTNET_API_KEY'),
        'secret': os.getenv('TESTNET_API_SECRET'),
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    exchange.set_sandbox_mode(True)

    trades = exchange.fetch_my_trades("ETH/USDT:USDT", limit=10)
    for t in trades:
        print(f"Trade {t['id']}: side={t['side']}, price={t['price']}, amount={t['amount']}")
        print(f"Info: PnL={t['info'].get('realizedPnl')}, Commission={t['info'].get('commission')}")
except Exception as e:
    print(f"Error: {e}")
