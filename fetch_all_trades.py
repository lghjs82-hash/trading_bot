import os
import pandas as pd
from execution_engine import ExecutionEngine
import config

def analyze():
    engine = ExecutionEngine()
    symbol = config.ETH_SYMBOL
    print(f"Analyzing trades for {symbol}...")
    
    try:
        # fetch_my_trades via CCXT
        trades = engine.exchange.fetch_my_trades(symbol, limit=200)
        if not trades:
            print("No trades found.")
            return

        records = []
        for t in trades:
            info = t.get('info', {})
            realized_pnl = float(info.get('realizedPnl', 0))
            if realized_pnl != 0:
                records.append({
                    'datetime': t['datetime'],
                    'side': t['side'], # 'buy' or 'sell'
                    'price': t['price'],
                    'amount': t['amount'],
                    'pnl': realized_pnl,
                    'is_close': (t['side'] == 'sell' and float(t['amount']) < 0) or (realized_pnl != 0) 
                })

        if not records:
            print("No trades with realized PnL.")
            return

        df = pd.DataFrame(records)
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        wins = df[df['pnl'] > 0]
        losses = df[df['pnl'] < 0]
        
        print("\n--- PERFORMANCE SUMMARY ---")
        print(f"Total Realized Trades: {len(df)}")
        print(f"Win Rate: {len(wins)/len(df)*100:.2f}% ({len(wins)}W / {len(losses)}L)")
        print(f"Total PnL: {df['pnl'].sum():.4f} USDT")
        
        if len(wins) > 0:
            print(f"Average Win: {wins['pnl'].mean():.4f} USDT")
        if len(losses) > 0:
            print(f"Average Loss: {losses['pnl'].mean():.4f} USDT")
            if abs(losses['pnl'].sum()) > 0:
                print(f"Profit Factor: {abs(wins['pnl'].sum() / losses['pnl'].sum()):.2f}")
        
        # Analyze by side (closing side)
        long_closes = df[df['side'].str.lower() == 'sell']
        short_closes = df[df['side'].str.lower() == 'buy']
        
        print(f"\nLong Exit PnL: {long_closes['pnl'].sum():.4f} USDT ({len(long_closes)} trades)")
        print(f"Short Exit PnL: {short_closes['pnl'].sum():.4f} USDT ({len(short_closes)} trades)")
        
        print("\n--- RECENT 10 TRADES ---")
        print(df.tail(10)[['datetime', 'side', 'price', 'amount', 'pnl']])

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze()
