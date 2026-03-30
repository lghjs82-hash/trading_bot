import ccxt
import logging
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ExecutionEngine")

class ExecutionEngine:
    def __init__(self):
        self.mode = config.ETH_MODE
        self.symbol = config.ETH_SYMBOL
        
        # Initialize exchange
        exchange_params = {
            'apiKey': config.BINANCE_API_KEY,
            'secret': config.BINANCE_API_SECRET,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'test': self.mode == "TESTNET"
            }
        }
        
        self.exchange = ccxt.binanceusdm(exchange_params)
        
        if self.mode == "TESTNET":
            if hasattr(self.exchange, 'enable_demo_trading'):
                self.exchange.enable_demo_trading(True)
                logger.info("Running in TESTNET mode - Demo trading enabled")
            else:
                self.exchange.set_sandbox_mode(True)
                logger.info("Running in TESTNET mode - Sandbox mode enabled (Legacy)")
        else:
            logger.warning("Running in LIVE mode!")
            
        # Debug: Log first 4 chars of API key
        key_snippet = str(self.exchange.apiKey)[:4] if self.exchange.apiKey else "None"
        logger.info(f"Initialized with API Key starting with: {key_snippet}")
        
        # Set margin mode
        self.set_margin_mode(self.symbol, config.ISOLATED_MARGIN)
        self._spot_exchange = None  # Lazy init for 1s charts

    def set_margin_mode(self, symbol, isolated: bool):
        margin_type = 'isolated' if isolated else 'crossed'
        try:
            self.exchange.set_margin_mode(margin_type, symbol)
            logger.info(f"Successfully set margin mode to {margin_type.upper()} for {symbol}")
        except Exception as e:
            if 'No need to change margin type' in str(e):
                logger.info(f"Margin mode already {margin_type.upper()} for {symbol}")
            else:
                logger.warning(f"Could not change margin mode to {margin_type.upper()}: {e}")

    def check_auth(self):
        """Verify API keys and connection"""
        try:
            balance = self.exchange.fetch_balance()
            # More robust balance logging (some accounts might not have USDT key if 0)
            usdt_total = balance.get('USDT', {}).get('total', 0.0)
            logger.info(f"Authentication successful. Total USDT: {usdt_total}")
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def get_market_data(self, symbol, interval="24h"):
        """Fetch current price and ticker info for a given interval"""
        try:
            if interval == "24h":
                ticker = self.exchange.fetch_ticker(symbol)
                return {
                    "last": ticker['last'],
                    "percentage": ticker['percentage'],
                    "high": ticker['high'],
                    "low": ticker['low'],
                    "volume": ticker['quoteVolume']
                }
            else:
                tf_map = {"1h": "1h", "4h": "4h", "1d": "1d"}
                tf = tf_map.get(interval, "1h")
                
                ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=2)
                if len(ohlcv) < 2:
                    ticker = self.exchange.fetch_ticker(symbol)
                    return {"last": ticker['last'], "percentage": 0, "high": ticker['high'], "low": ticker['low'], "volume": ticker['quoteVolume']}
                
                prev_close = ohlcv[0][4]
                curr_close = ohlcv[1][4]
                pct_change = ((curr_close - prev_close) / prev_close) * 100
                
                # Use current candle [timestamp, open, high, low, close, volume]
                current_candle = ohlcv[1]
                
                return {
                    "last": current_candle[4],
                    "percentage": round(pct_change, 2),
                    "high": current_candle[2],
                    "low": current_candle[3],
                    "volume": current_candle[5]
                }
        except Exception as e:
            logger.error(f"Error fetching market data ({interval}): {e}")
            return None

    def get_ohlcv(self, symbol, timeframe='1h', limit=100):
        """Fetch historical OHLCV data. Fallback to Spot for 1s as Futures doesn't support it."""
        try:
            target_exchange = self.exchange
            
            # 1s is only supported on Spot for Binance
            if timeframe == '1s':
                if not self._spot_exchange:
                    self._spot_exchange = ccxt.binance({
                        'enableRateLimit': True,
                        'options': {'defaultType': 'spot'}
                    })
                target_exchange = self._spot_exchange
                # Cleanup symbol for spot (ETH/USDT:USDT -> ETH/USDT)
                symbol = symbol.split(':')[0]

            ohlcv = target_exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            return [{
                "t": o[0],
                "o": o[1],
                "h": o[2],
                "l": o[3],
                "c": o[4],
                "v": o[5]
            } for o in ohlcv]
        except Exception as e:
            logger.error(f"Error fetching OHLCV: {e}")
            return []

    def get_trade_history(self, symbol, limit=100):
        """Fetch recent executed trade history and consolidate them into entry/exit sessions"""
        try:
            # Increase limit for more accurate matching of pairs
            trades = self.exchange.fetch_my_trades(symbol, limit=200)
            if not trades:
                return []
            
            # Sort by time ascending to process chronologically
            trades.sort(key=lambda x: x['timestamp'])
            
            sessions = []
            current_session = None
            
            for t in trades:
                price = float(t.get('price', 0))
                amount = float(t.get('amount', 0))
                timestamp = t.get('timestamp', 0)
                info = t.get('info', {})
                realized_pnl = float(info.get('realizedPnl', 0))
                raw_side = str(t.get('side', '')).lower()
                
                # In one-way mode, realized_pnl is reported on trades that reduce position.
                is_exit = (realized_pnl != 0)
                
                if not is_exit:
                    # This is likely an ENTRY (Open)
                    # If side matches current session, it's adding to it; else start new
                    side_str = 'LONG' if raw_side == 'buy' else 'SHORT'
                    
                    if current_session and current_session['side'] == side_str:
                        # Scaling in
                        total_cost = (current_session['entry_price'] * current_session['amount']) + (price * amount)
                        current_session['amount'] += amount
                        current_session['entry_price'] = total_cost / current_session['amount']
                    else:
                        # New session
                        current_session = {
                            "entry_time": timestamp,
                            "entry_price": price,
                            "amount": amount,
                            "side": side_str,
                            "entry_order_id": t.get('order', t.get('orderId', 'N/A')),
                            "exit_time": None,
                            "exit_price": 0,
                            "exit_order_id": None,
                            "realized_pnl": 0,
                            "roi": 0,
                            "status": "OPEN"
                        }
                else:
                    # This is an EXIT (Close)
                    if current_session:
                        # Complete the session (or partial)
                        current_session['exit_time'] = timestamp
                        current_session['exit_price'] = price
                        current_session['exit_order_id'] = t.get('order', t.get('orderId', 'N/A'))
                        current_session['realized_pnl'] += realized_pnl
                        current_session['status'] = "CLOSED"
                        
                        # Estimate ROI for the session (cumulative) using current leverage (default 20x)
                        # realized_pnl is based on the difference from existing average price
                        margin = (current_session['entry_price'] * amount) / 20.0 if current_session['entry_price'] > 0 else 0
                        trade_roi = (realized_pnl / margin) * 100 if margin > 0 else 0
                        current_session['roi'] = round(trade_roi, 2)
                        
                        # Update remaining amount in current session
                        current_session['amount'] -= amount
                        
                        # Save session clone (as it is now 'completed' for this trade)
                        # If multiple partial exits, we'll see multiple lines or one summary?
                        # User wants "Entry/Exit shown on one line".
                        # Let's add it to results.
                        sessions.append(current_session.copy())
                        
                        if current_session['amount'] <= 0.0001: # Effectively closed
                            current_session = None
                    else:
                        # Exit without found entry (maybe entry was outside limit)
                        side_str = 'SHORT' if raw_side == 'buy' else 'LONG'
                        sessions.append({
                            "entry_time": 0,
                            "entry_price": 0,
                            "amount": amount,
                            "side": side_str,
                            "entry_order_id": "N/A",
                            "exit_time": timestamp,
                            "exit_price": price,
                            "exit_order_id": t.get('order', t.get('orderId', 'N/A')),
                            "realized_pnl": realized_pnl,
                            "roi": 0,
                            "status": "CLOSED (ORPHAN)"
                        })
            
            # Show newest sessions first for the UI
            return sorted(sessions, key=lambda x: max(x['entry_time'], x['exit_time'] or 0), reverse=True)[:limit]
            
        except Exception as e:
            logger.error(f"Error fetching trade history: {e}")
            return []

    def get_available_symbols(self):
        """Fetch all USDT-M Futures symbols"""
        try:
            markets = self.exchange.load_markets()
            # Filter for USDT-M futures using robust .get()
            symbols = [s for s, m in markets.items() if m.get('active') and m.get('quote') == 'USDT' and m.get('futures')]
            return sorted(symbols)
        except Exception as e:
            logger.error(f"Error fetching symbols: {e}")
            return ["ETH/USDT:USDT", "BTC/USDT:USDT"]

    def get_market_price(self):
        """Get current ticker price"""
        try:
            ticker = self.exchange.fetch_ticker(self.symbol)
            return ticker['last']
        except Exception as e:
            logger.error(f"Error fetching market price: {e}")
            return None

    def place_order(self, side, amount, price=None, stop_loss=None, take_profit=None):
        """Place a market or limit order, then optionally attach stop-loss and take-profit orders"""
        try:
            if price is not None: price = float(self.exchange.price_to_precision(self.symbol, price))
            if stop_loss is not None: stop_loss = float(self.exchange.price_to_precision(self.symbol, stop_loss))
            if take_profit is not None: take_profit = float(self.exchange.price_to_precision(self.symbol, take_profit))
            
            order_type = 'market' if price is None else 'limit'
            
            logger.info(f"Placing {order_type} {side} order for {amount} {self.symbol}")
            
            # Map LONG/SHORT to buy/sell for Binance API compatibility
            api_side = 'buy' if side.upper() == 'LONG' else 'sell' if side.upper() == 'SHORT' else side.lower()
            
            order = self.exchange.create_order(
                symbol=self.symbol,
                type=order_type,
                side=api_side,
                amount=amount,
                price=price,
                params={}
            )
            
            logger.info(f"Order placed successfully: {order['id']}")
            
            # Opposite side for exit orders
            exit_side = 'sell' if side.upper() == 'LONG' else 'buy'

            # 1. Attach Stop-Loss order
            if stop_loss and order:
                try:
                    sl_order = self.exchange.create_order(
                        symbol=self.symbol,
                        type='STOP_MARKET',
                        side=exit_side,
                        amount=amount,
                        price=None,
                        params={
                            'stopPrice': stop_loss,
                            'reduceOnly': True
                        }
                    )
                    logger.info(f"Stop-loss order placed: {sl_order.get('id', 'N/A')} @ {stop_loss}")
                except Exception as sl_e:
                    logger.warning(f"Could not place stop-loss order: {sl_e}")
            
            # 2. Attach Take-Profit order
            if take_profit and order:
                try:
                    tp_order = self.exchange.create_order(
                        symbol=self.symbol,
                        type='TAKE_PROFIT_MARKET',
                        side=exit_side,
                        amount=amount,
                        price=None,
                        params={
                            'stopPrice': take_profit,
                            'reduceOnly': True
                        }
                    )
                    logger.info(f"Take-profit order placed: {tp_order.get('id', 'N/A')} @ {take_profit}")
                except Exception as tp_e:
                    logger.warning(f"Could not place take-profit order: {tp_e}")
            
            return order
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            return None

    def place_order_stops(self, side: str, amount: float, stop_loss: float = None, take_profit: float = None, cancel_old: bool = True):
        """Place/Update consolidated Stop Loss and Take Profit orders for the entire position"""
        try:
            if stop_loss is not None: stop_loss = float(self.exchange.price_to_precision(self.symbol, stop_loss))
            if take_profit is not None: take_profit = float(self.exchange.price_to_precision(self.symbol, take_profit))

            if cancel_old:
                self.cancel_all_orders()
            
            # Create Stop Loss Market order
            if stop_loss:
                sl_side = 'sell' if side.lower() == 'buy' or side.upper() == 'LONG' else 'buy'
                self.exchange.create_order(
                    symbol=self.symbol,
                    type='STOP_MARKET',
                    side=sl_side,
                    amount=amount,
                    params={
                        'stopPrice': stop_loss,
                        'reduceOnly': True
                    }
                )
                logger.info(f"Consolidated Stop Loss set at {stop_loss} for {amount} {self.symbol}")

            # Create Take Profit Market order
            if take_profit:
                tp_side = 'sell' if side.lower() == 'buy' or side.upper() == 'LONG' else 'buy'
                self.exchange.create_order(
                    symbol=self.symbol,
                    type='TAKE_PROFIT_MARKET',
                    side=tp_side,
                    amount=amount,
                    params={
                        'stopPrice': take_profit,
                        'reduceOnly': True
                    }
                )
                logger.info(f"Consolidated Take Profit set at {take_profit} for {amount} {self.symbol}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to update consolidated stops: {e}")
            return False

    def update_stop_loss(self, side: str, amount: float, new_stop_price: float):
        """Update existing Stop Loss order to a new price (e.g. Break-Even)"""
        try:
            if new_stop_price is not None:
                new_stop_price = float(self.exchange.price_to_precision(self.symbol, new_stop_price))
            
            # Cancel only STOP_MARKET orders
            open_orders = self.get_open_orders()
            for o in open_orders:
                if o.get('type') == 'STOP_MARKET':
                    self.exchange.cancel_order(o['id'], self.symbol)
                    logger.info(f"Cancelled old Stop Loss ({o['id']}) to move to {new_stop_price}")
            
            # Place new SL
            sl_side = 'sell' if side.upper() == 'LONG' or side.lower() == 'buy' else 'buy'
            return self.exchange.create_order(
                symbol=self.symbol,
                type='STOP_MARKET',
                side=sl_side,
                amount=amount,
                params={
                    'stopPrice': new_stop_price,
                    'reduceOnly': True
                }
            )
        except Exception as e:
            logger.error(f"Failed to update stop loss: {e}")
            return None

    def get_open_orders(self):
        """Fetch all open orders for the current symbol"""
        try:
            return self.exchange.fetch_open_orders(self.symbol)
        except Exception as e:
            logger.error(f"Error fetching open orders: {e}")
            return []

    def cancel_all_orders(self):
        """Cancel all open orders for the symbol"""
        try:
            return self.exchange.cancel_all_orders(self.symbol)
        except Exception as e:
            logger.error(f"Error cancelling orders: {e}")
            return None

    def get_order_book(self, symbol, limit=10):
        """Fetch current order book (bids and asks)"""
        try:
            ob = self.exchange.fetch_order_book(symbol, limit=limit)
            bids = ob.get('bids', [])
            asks = ob.get('asks', [])
            
            result = {
                "bids": bids,
                "asks": asks
            }
            
            # Calculate Imbalance
            total_bids = sum([b[1] for b in bids]) if bids else 0
            total_asks = sum([a[1] for a in asks]) if asks else 0
            
            if total_bids + total_asks > 0:
                result["imbalance"] = {
                    "bids_pct": round((total_bids / (total_bids + total_asks)) * 100, 2),
                    "asks_pct": round((total_asks / (total_bids + total_asks)) * 100, 2)
                }
            return result
        except Exception as e:
            err_msg = str(e)
            # Filter out long HTML from 502/504 errors
            if "<html>" in err_msg.lower() or "502" in err_msg or "504" in err_msg:
                logger.warning(f"Error fetching order book: Binance Server Temporary Unavailable (502/504)")
            else:
                logger.error(f"Error fetching order book: {err_msg}")
            return {"bids": [], "asks": []}

    def get_futures_metrics(self, symbol):
        """Fetch Funding Rate and Open Interest"""
        metrics = {"funding_rate": 0.0, "open_interest": 0.0}
        try:
            # Funding rate
            fr = self.exchange.fetch_funding_rate(symbol)
            if fr and "fundingRate" in fr:
                metrics["funding_rate"] = round(float(fr["fundingRate"]) * 100, 4)
            
            # Open Interest
            oi = self.exchange.fetch_open_interest(symbol)
            if oi and "openInterestAmount" in oi:
                metrics["open_interest"] = float(oi["openInterestAmount"])
            elif oi and "baseVolume" in oi:
                metrics["open_interest"] = float(oi["baseVolume"])
        except Exception as e:
            err_msg = str(e)
            if "<html>" in err_msg.lower() or "502" in err_msg:
                logger.debug(f"Error fetching futures metrics for {symbol}: Binance Server Error (502)")
            else:
                logger.warning(f"Error fetching futures metrics for {symbol}: {err_msg}")
            
        return metrics

    def get_position(self, symbol: str) -> dict:
        """Fetch current active position for the symbol using v2 API (required for Demo Trading)"""
        default = {"side": "FLAT", "size": 0, "entry_price": 0, "unrealized_pnl": 0, "leverage": 1}
        try:
            # Convert ccxt symbol to Binance raw symbol (ETH/USDT:USDT -> ETHUSDT)
            raw_symbol = symbol.replace("/", "").replace(":USDT", "")
            
            # fapiPrivateV2 works correctly with Demo Trading (v1 doesn't)
            positions = self.exchange.fapiPrivateV2GetPositionRisk({"symbol": raw_symbol})
            
            for pos in positions:
                position_amt = float(pos.get("positionAmt", 0))
                if position_amt == 0:
                    continue  # No active position
                
                position_side = pos.get("positionSide", "BOTH")
                
                # Determine direction
                if position_side == "LONG":
                    side = "LONG"
                elif position_side == "SHORT":
                    side = "SHORT"
                else:  # BOTH (one-way mode)
                    side = "LONG" if position_amt > 0 else "SHORT"
                
                return {
                    "side": side,
                    "size": abs(position_amt),
                    "entry_price": float(pos.get("entryPrice", 0)),
                    "unrealized_pnl": float(pos.get("unRealizedProfit", 0)),
                    "leverage": float(pos.get("leverage", 1))
                }
            
            return default
        except Exception as e:
            err_msg = str(e)
            if "<html>" in err_msg.lower() or "502" in err_msg:
                logger.warning(f"Error fetching position for {symbol}: Binance Server Error (502)")
            else:
                logger.error(f"Error fetching position for {symbol}: {err_msg}")
            return default

    def get_total_balance(self):
        """Fetch total USDT wallet balance from Binance Futures"""
        try:
            balance = self.exchange.fetch_balance()
            return balance.get('USDT', {}).get('total', 0.0)
        except Exception as e:
            err_msg = str(e)
            if "<html>" in err_msg.lower() or "502" in err_msg:
                logger.warning(f"Error fetching balance: Binance Server Error (502)")
            else:
                logger.error(f"Error fetching balance: {err_msg}")
            return 0.0

    def close_position(self, symbol):
        """Force close any open position for the symbol at market price"""
        try:
            pos = self.get_position(symbol)
            if pos["side"] == "FLAT" or pos["size"] == 0:
                logger.info(f"No active position to close for {symbol}")
                return True
            
            # Place opposite market order to close
            side = 'sell' if pos["side"] == "LONG" else 'buy'
            logger.info(f"FORCE EXIT: closing {pos['side']} position of {pos['size']} {symbol}")
            
            order = self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=pos["size"],
                params={'reduceOnly': True}
            )
            
            # Also cancel all remaining orders (like stop losses)
            self.cancel_all_orders()
            
            return order
        except Exception as e:
            err_msg = str(e)
            if "<html>" in err_msg.lower() or "502" in err_msg:
                logger.error(f"Error in close_position: Binance Server Error (502)")
            else:
                logger.error(f"Error in close_position: {err_msg}")
            return None
