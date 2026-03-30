import os
import ccxt
import pandas as pd
from dotenv import load_dotenv

load_dotenv('/Users/seong-woo/Desktop/binance-trading-bot/.env', override=True)

exchange = ccxt.binanceusdm({
    'apiKey': os.getenv('TESTNET_API_KEY'),
    'secret': os.getenv('TESTNET_API_SECRET'),
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})
exchange.set_sandbox_mode(True)
symbol = "ETH/USDT:USDT"

try:
    trades = exchange.fetch_my_trades(symbol, limit=200)
    if not trades:
        print("No trades found.")
        exit(0)
    
    records = []
    for t in trades:
        info = t.get('info', {})
        realized_pnl = float(info.get('realizedPnl', 0))
        if realized_pnl != 0:
            records.append({
                'datetime': t['datetime'],
                'side': t['side'],
                'price': t['price'],
                'amount': t['amount'],
                'pnl': realized_pnl
            })
            
    if not records:
        print("No trades with realized PnL.")
        exit(0)

    df = pd.DataFrame(records)
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    wins = df[df['pnl'] > 0]
    losses = df[df['pnl'] < 0]
    
    print(f"Total Closed Trades Analyzed: {len(df)}")
    print(f"Win Rate: {len(wins)/len(df)*100:.2f}% ({len(wins)}W / {len(losses)}L)")
    print(f"Total PnL: {df['pnl'].sum():.4f} USDT")
    
    if len(wins) > 0:
        print(f"Average Win: {wins['pnl'].mean():.4f} USDT")
    if len(losses) > 0:
        print(f"Average Loss: {losses['pnl'].mean():.4f} USDT")
        print(f"Profit Factor: {abs(wins['pnl'].sum() / losses['pnl'].sum()):.2f}")
        
    long_pnl = df[df['side'] == 'sell']['pnl'].sum() # PnL is realized when closing a trade (sell closes long)
    short_pnl = df[df['side'] == 'buy']['pnl'].sum() # PnL is realized when buying closes a short
    print(f"PnL when closing Longs (Sell): {long_pnl:.4f} USDT")
    print(f"PnL when closing Shorts (Buy): {short_pnl:.4f} USDT")
    
except Exception as e:
    print(f"Error: {e}")
