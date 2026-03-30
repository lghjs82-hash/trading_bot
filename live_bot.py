import time
import logging
import pandas as pd
import json
from execution_engine import ExecutionEngine
from strategies.structure_shock import StructureShockStrategy
from strategies.ema_crossover import EMACrossoverStrategy
from strategies.rsi_reversion import RSIReversionStrategy
from strategies.macd_trend import MACDTrendStrategy
from strategies.custom_strategy import CustomStrategy
from strategies.active_scalper import ActiveScalperStrategy
from strategies.multi_filter_momentum import MultiFilterMomentumStrategy
from telegram_notifier import TelegramNotifier
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LiveBot")

class LiveBot:
    def __init__(self):
        self.engine = ExecutionEngine()
        self.strategy = self._load_strategy()
        self.notifier = TelegramNotifier(
            token=getattr(config, 'TELEGRAM_BOT_TOKEN', ''),
            chat_id=getattr(config, 'TELEGRAM_CHAT_ID', '')
        )
        self.symbol = config.ETH_SYMBOL
        self.timeframe = config.ETH_TIMEFRAME
        self.isolated_margin = config.ISOLATED_MARGIN
        self.is_running = False
        self.entries_count = 0  # Track number of entries for scaling in
        self.breakeven_triggered = False  # Track if SL was moved to Entry price

    def _load_strategy(self):
        """Dynamically load the strategy based on current configuration"""
        strategy_name = config.ACTIVE_STRATEGY
        if strategy_name == "EMACrossover":
            return EMACrossoverStrategy()
        elif strategy_name == "RSIReversion":
            return RSIReversionStrategy()
        elif strategy_name == "MACDTrend":
            return MACDTrendStrategy()
        elif strategy_name == "Custom" or strategy_name == "CustomStrategy":
            return CustomStrategy()
        elif strategy_name == "ActiveScalper":
            return ActiveScalperStrategy()
        elif strategy_name == "MultiFilterMomentum":
            return MultiFilterMomentumStrategy()
        else:
            return StructureShockStrategy()

    def fetch_data(self):
        """Fetch latest OHLCV data from Binance"""
        try:
            # Fetch last 200 bars to calculate indicators
            ohlcv = self.engine.exchange.fetch_ohlcv(self.symbol, timeframe=self.timeframe, limit=500)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return None

    def update_state(self, status="Running", logs=None):
        """Update the shared state file for the dashboard"""
        try:
            current_balance = self.engine.get_total_balance()
            if current_balance <= 0:
                current_balance = config.INITIAL_CAPITAL
        except:
            current_balance = config.INITIAL_CAPITAL

        state = {
            "status": status,
            "balance": current_balance,
            "pnl": 0.0,
            "strategy": self.strategy.name,
            "symbol": self.symbol,
            "logs": logs or ["Bot is active."]
        }
        with open("dashboard_state.json", "w") as f:
            json.dump(state, f)

    def run_once(self):
        """Main loop iteration"""
        from macro_service import MacroService
        macro_svc = MacroService()
        news_risk = macro_svc.get_news_sentiment()
        pos_momentum = macro_svc.get_positive_momentum()
        
        # 1. Fetch data
        df = self.fetch_data()
        if df is None: return

        # 2. Add indicators
        df = self.strategy.add_indicators(df)

        # 3. Detect signals
        signals = self.strategy.detect_signals(df)
        
        # Log to state
        logs = [f"Checking for signals at {time.strftime('%H:%M:%S')}"]
        
        # --- Safety Guard Check ---
        risk_score = news_risk.get("score", 0)
        threshold = getattr(config, 'SAFETY_GUARD_THRESHOLD', 101)
        if risk_score > threshold:
            logs.append(f"⚠️ SAFETY GUARD ACTIVE: Extreme Geopolitical Risk ({risk_score}/100) detected.")
            logs.append("Halting new entries to prevent panic-sell liquidation.")
            self.update_state(logs=logs)
            return
            
        # --- Positive News Momentum (Golden Cross) ---
        if pos_momentum.get("golden_cross"):
            logs.append(f"🟢 POSITIVE MOMENTUM GOLDEN CROSS DETECTED! (Short-Term: {pos_momentum.get('sta_hits')} vs Long-Term: {pos_momentum.get('lta_hits')})")
            logs.append("Triggering high-conviction LONG entry signal based on Institutional/Corporate news surge.")
            # Bug Fix: append a proper signal dict, not a string
            macro_signal_exists = any(isinstance(s, dict) and s.get("reason") == "MACRO_GOLDEN_CROSS" for s in signals)
            if not macro_signal_exists:
                try:
                    ticker = self.engine.exchange.fetch_ticker(self.symbol)
                    current_price = float(ticker['last'])
                    signals.append({
                        "side": "LONG",
                        "entry_price": current_price,
                        "stop_price": current_price * 0.985,  # 1.5% stop
                        "reason": "MACRO_GOLDEN_CROSS"
                    })
                except Exception as e:
                    logger.warning(f"Could not get price for macro signal: {e}")
        
        if signals:
            logs.append(f"Detected signals: {signals}")
        self.update_state(logs=logs)

        self.update_state(logs=logs)

        # 4. Handle Exit Strategies
        current_pos = {"side": "FLAT"}
        try:
            current_pos = self.engine.get_position(self.symbol)
        except Exception as e:
            logger.warning(f"Could not fetch positions: {e}")

        # Check for Indicator, Percent, or Global TP exits
        if current_pos["side"] != "FLAT":
            exit_triggered = False
            exit_reason = ""
            
            # --- GLOBAL TAKE PROFIT (ROI Based - Enhanced 1s monitoring) ---
            entry_price = float(current_pos.get("entry_price", 0))
            if entry_price > 0:
                # Use current ticker price for Tick-by-Tick monitoring
                try:
                    last_price = self.engine.get_market_price() or float(df.iloc[-1]["close"])
                except:
                    last_price = float(df.iloc[-1]["close"])
                
                leverage = current_pos.get("leverage", 20)
                
                if current_pos["side"] == "LONG":
                    price_change_pct = ((last_price - entry_price) / entry_price) * 100
                else:
                    price_change_pct = ((entry_price - last_price) / entry_price) * 100
                    
                actual_roi = price_change_pct * leverage
                
                # Log current ROI to console for transparency
                roi_msg = f"[ROI Watch] {self.symbol} | Current: {actual_roi:.2f}% | Target: {config.GLOBAL_TP_PCT:.1f}%"
                logger.info(roi_msg)
                logs.append(roi_msg)
                
                # Check if GREATER than or EQUAL to target (matches user request)
                if actual_roi >= config.GLOBAL_TP_PCT:
                    exit_triggered = True
                    exit_reason = f"Global ROI Target Reached/Exceeded: {actual_roi:.2f}% (Price Change: {price_change_pct:.2f}%)"
            
            # --- PERCENT Exit (ROI Based) ---
            if not exit_triggered and config.EXIT_STRATEGY_MODE == "PERCENT":
                notional = current_pos.get("notional", current_pos.get("size", 0) * current_pos.get("entry_price", 0))
                leverage = current_pos.get("leverage", 1)
                margin = notional / leverage if leverage > 0 else notional
                roi = (current_pos.get("unrealized_pnl", 0) / margin) * 100 if margin > 0 else 0
                
                if roi >= config.EXIT_PROFIT_PCT:
                    exit_triggered = True
                    exit_reason = f"Profit Target Reached: {roi:.2f}%"
            
            # --- INDICATOR Exit ---
            elif config.EXIT_STRATEGY_MODE == "INDICATOR":
                if self.strategy.check_exit_condition(df, current_pos["side"]):
                    exit_triggered = True
                    exit_reason = f"Indicator Exit Signal ({config.ACTIVE_STRATEGY})"

            if exit_triggered:
                logs.append(f"💰 AUTOMATIC EXIT TRIGGERED: {exit_reason}")
                close_order = self.engine.close_position(self.symbol)
                if close_order:
                    logs.append("✅ Position closed successfully.")
                    self.notifier.send(f"💰 *Automatic Exit Success!*\nSymbol: {self.symbol}\nReason: {exit_reason}")
                    current_pos["side"] = "FLAT" # Mark as flat so entries can proceed
                    self.entries_count = 0 # Reset entries count
                    self.breakeven_triggered = False # Reset BE flag
                else:
                    logs.append("❌ Failed to close position.")

            # --- BREAK-EVEN Logic (Risk Management) ---
            if not self.breakeven_triggered and current_pos["side"] != "FLAT":
                entry_price = current_pos["entry_price"]
                last_price = df.iloc[-1]["close"]
                
                # Fetch TP price from open orders
                open_orders = self.engine.get_open_orders()
                tp_price = 0
                for o in open_orders:
                    if o.get('type') == 'TAKE_PROFIT_MARKET':
                        tp_price = float(o.get('stopPrice', 0))
                
                if tp_price > 0:
                    tp_distance = abs(tp_price - entry_price)
                    current_profit_distance = abs(last_price - entry_price)
                    
                    # Move to BE if 50% of the way to TP
                    if current_profit_distance >= (tp_distance * 0.5):
                        is_profit = (current_pos["side"] == "LONG" and last_price > entry_price) or \
                                    (current_pos["side"] == "SHORT" and last_price < entry_price)
                        
                        if is_profit:
                            logs.append(f"🛡️ PROTECTING CAPITAL: Moving SL to Break-Even ({entry_price})")
                            if self.engine.update_stop_loss(current_pos["side"], current_pos["size"], entry_price):
                                self.breakeven_triggered = True
                                self.notifier.send(f"🛡️ *Break-Even Triggered*\nSymbol: `{self.symbol}`\nSL moved to entry: `{entry_price}`")
                            else:
                                logs.append("❌ Failed to update SL to Break-Even.")

        # 5. Execute Entry signals
        if not signals:
            self.update_state(logs=logs)
            return

        # Check for FLIP exit (if new signal is contrary to current position)
        if current_pos["side"] != "FLAT" and config.EXIT_STRATEGY_MODE == "FLIP":
            new_side = signals[0]["side"]
            if new_side != current_pos["side"]:
                logs.append(f"🔄 FLIP EXIT: New {new_side} signal contrary to current {current_pos['side']} position.")
                if self.engine.close_position(self.symbol):
                    logs.append("✅ Flipped position: old closed. Proceeding to entry.")
                    self.notifier.send(f"🔄 *FLIP Exit Triggered*\nSymbol: `{self.symbol}`\nClosed old `{current_pos['side']}` to enter `{new_side}`.")
                    current_pos["side"] = "FLAT"

        # Final Entry Guard
        if current_pos["side"] != "FLAT":
            # Multiple Entry Check
            new_side = signals[0]["side"]
            if config.ALLOW_MULTIPLE_ENTRIES and new_side == current_pos["side"]:
                if self.entries_count >= config.MAX_ENTRIES_COUNT:
                    logs.append(f"Maximum entries reached ({self.entries_count}). Skipping further scaling.")
                    self.update_state(logs=logs)
                    return
                logs.append(f"Position active ({current_pos['side']}), but scaling in allowed ({self.entries_count}/{config.MAX_ENTRIES_COUNT}).")
            else:
                logs.append(f"Position already active ({current_pos['side']}). Skipping new entry.")
                self.update_state(logs=logs)
                return

        for signal in signals:
            # Bug Fix: skip any non-dict entries (defensive guard)
            if not isinstance(signal, dict):
                logger.warning(f"Skipping invalid signal (not a dict): {signal}")
                continue
            side = signal["side"] # LONG or SHORT
            
            # Calculate amount based on risk
            try:
                balance = self.engine.get_total_balance()
                if balance <= 0: balance = config.INITIAL_CAPITAL
                
                risk_amount = balance * config.RISK_PER_TRADE_PCT
                price = signal["entry_price"]
                stop_loss = signal["stop_price"]
                
                # Risk per contract
                risk_per_unit = abs(price - stop_loss)
                if risk_per_unit > 0:
                    amount = risk_amount / risk_per_unit
                else:
                    amount = (balance * 0.1) / price # Fallback 10% of balance
                
                # Apply Min Notional (Binance Testnet requires ~100 USDT)
                MIN_NOTIONAL = 101.0
                if (amount * price) < MIN_NOTIONAL:
                    amount = MIN_NOTIONAL / price
                    logs.append(f"⚠️ Boosting investment to meet Min Notional requirements: ${MIN_NOTIONAL}")

                # Apply Max Investment Limit
                if (amount * price) > config.MAX_INVESTMENT_AMOUNT:
                    amount = config.MAX_INVESTMENT_AMOUNT / price
                    logs.append(f"⚠️ Capping investment to limit: ${config.MAX_INVESTMENT_AMOUNT}")

                # Round amount based on symbol precision (simplified)
                amount = self.engine.exchange.amount_to_precision(self.symbol, amount)
                amount = float(amount)
                
                if amount > 0:
                    logs.append(f"🚀 EXECUTING {side} ORDER: {amount} units at {price}")
                    order = self.engine.place_order(
                        side=side, 
                        amount=amount, 
                        stop_loss=stop_loss,
                        take_profit=signal.get("take_profit")
                    )
                    if order:
                        order_id = order.get('id', 'N/A')
                        logs.append(f"✅ Order Success: {order_id}")
                        self.entries_count += 1
                        
                        # Consolidated Stop Management (if multiple entries)
                        if self.entries_count > 1:
                            # Re-fetch position to get latest total size and entry price
                            updated_pos = self.engine.get_position(self.symbol)
                            if updated_pos["side"] != "FLAT":
                                logs.append(f"♻️ Updating consolidated SL/TP for total size: {updated_pos['size']}")
                                # Re-calculate SL/TP based on new weighted average entry price
                                # For simplicity, we use the original RR ratios from the latest signal applied to the *entire* position
                                self.engine.place_order_stops(
                                    side=side,
                                    amount=updated_pos["size"],
                                    stop_loss=stop_loss,
                                    take_profit=signal.get("take_profit"),
                                    cancel_old=True
                                )
                        # Send Telegram notification
                        self.notifier.notify_order(
                            side=side,
                            symbol=self.symbol,
                            amount=amount,
                            price=price,
                            order_id=str(order_id),
                            mode=config.ETH_MODE
                        )
                    else:
                        logs.append(f"❌ Order Failed for {side}")
                        self.notifier.notify_error(self.symbol, f"Order placement returned None for {side}")
                else:
                    logs.append(f"⚠️ Calculated amount too small: {amount}")
                    
            except Exception as e:
                logs.append(f"❌ Execution Error: {e}")
                logger.error(f"Execution Error: {e}")
                self.notifier.notify_error(self.symbol, str(e))
            
            # Only process the first signal per bar
            break
            
        self.update_state(logs=logs)

    def start(self):
        """Start the bot loop with self-healing (auto-restart on crash)"""
        self.is_running = True
        
        while self.is_running:
            try:
                logger.info(f"Authenticating for {config.BOT_NAME}...")
                
                # Retry loop for initial authentication
                max_retries = 10
                retry_count = 0
                while not self.engine.check_auth():
                    if not self.is_running: return
                    
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error("Authentication failed permanently. Retrying in 60s...")
                        self.update_state(status="Error", logs=["Auth failed. Waiting 60s to retry..."])
                        time.sleep(60)
                        retry_count = 0 # Reset to try again later
                        continue
                    
                    logger.warning(f"Authentication attempt {retry_count}/{max_retries} failed. Retrying in 10s...")
                    self.update_state(status="Retrying", logs=[f"Auth failed. Retrying ({retry_count}/{max_retries})..."])
                    time.sleep(10)
                    
                    from execution_engine import ExecutionEngine
                    self.engine = ExecutionEngine()

                logger.info(f"Starting {config.BOT_NAME} on {config.ETH_SYMBOL}")
                self.update_state(status="Running")
                self._log_lifecycle_event("START", f"Bot initialized on {config.ETH_SYMBOL}")
                self.notifier.send(f"🟢 *Bot Started / Resumed*\nSymbol: `{config.ETH_SYMBOL}`")
                
                # Internal Bot Loop
                while self.is_running:
                    try:
                        config.reload()
                        # Monitoring loop (every 2s, 15 times = 30s approximate cycle)
                        for i in range(15):
                            if not self.is_running: break
                            self.run_once()
                            time.sleep(2)
                    except Exception as e:
                        logger.error(f"Error in internal loop: {e}")
                        self.update_state(logs=[f"Internal Error: {e}"])
                        time.sleep(5)
                        # Continue inner loop if it's a minor error, 
                        # or it will break to outer loop if critical
            
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                self.is_running = False
                self.update_state(status="Stopped")
                break
            except Exception as e:
                logger.error(f"CRITICAL: Bot thread crashed: {e}. Auto-restarting in 10s...")
                self.update_state(status="Recovering", logs=[f"Bot crashed: {e}. Restarting..."])
                self._log_lifecycle_event("RECOVERY", f"Auto-restart due to: {str(e)[:50]}...")
                time.sleep(10)
                # Outer loop will naturally restart the process

    def update_state(self, status=None, logs=None):
        """Update current bot state and write to shared JSON file (Atomic write for Windows)"""
        try:
            import tempfile
            import os
            
            # 1. Load existing state
            state = {}
            if os.path.exists(config.STATE_FILE):
                try:
                    with open(config.STATE_FILE, "r") as f:
                        state = json.load(f)
                except:
                    state = {}

            # 2. Update values
            if status: state["status"] = status
            if logs:
                current_logs = state.get("logs", [])
                # Keep only last 20 logs
                state["logs"] = (current_logs + logs)[-20:]

            # 3. Safe Atomic Write (Temp file -> Rename)
            # This prevents Windows "PermissionError" or "file in use" during read/write
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(config.STATE_FILE), suffix=".tmp")
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(state, f)
                # On Windows, replace can fail if target is open, but it's much faster than normal open()
                os.replace(temp_path, config.STATE_FILE)
            except Exception as e:
                if os.path.exists(temp_path): os.remove(temp_path)
                logger.debug(f"State update collision: {e}")
                
        except Exception as e:
            logger.debug(f"Critical error in update_state: {e}")

    def _log_lifecycle_event(self, event, details=""):
        """Record start/stop/recovery events to a persistent JSON file"""
        try:
            from datetime import datetime
            
            history = []
            if os.path.exists(config.LIFECYCLE_FILE):
                try:
                    with open(config.LIFECYCLE_FILE, "r") as f:
                        history = json.load(f)
                except:
                    history = []
            
            new_event = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "event": event,
                "details": details,
                "strategy": config.ACTIVE_STRATEGY,
                "symbol": config.ETH_SYMBOL,
                "mode": config.ETH_MODE
            }
            
            # Prepend newest
            history = [new_event] + history
            # Keep last 100
            history = history[:100]
            
            with open(config.LIFECYCLE_FILE, "w") as f:
                json.dump(history, f)
        except Exception as e:
            logger.error(f"Failed to log lifecycle event: {e}")

    def stop(self):
        """Stop the bot loop"""
        logger.info("Stopping bot...")
        self.is_running = False
        self.update_state(status="Stopped")
        self._log_lifecycle_event("STOP", "Manual stop triggered via Dashboard/Telegram")
        self.notifier.send(
            f"🔴 *Bot Stopped!*\n"
            f"━━━━━━━━━━━━━━\n"
            f"📌 Symbol: `{config.ETH_SYMBOL}`\n"
            f"🏷️ Mode: {'🧪 TESTNET' if config.ETH_MODE == 'TESTNET' else '💰 LIVE'}"
        )

if __name__ == "__main__":
    bot = LiveBot()
    bot.start()
