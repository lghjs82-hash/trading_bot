from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import json
import logging
from execution_engine import ExecutionEngine
import config

import threading
from live_bot import LiveBot
from macro_service import MacroService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Dashboard")

app = FastAPI()

_shared_engine = None
_shared_macro_svc = MacroService()

def get_engine():
    global _shared_engine
    if _shared_engine is None:
        _shared_engine = ExecutionEngine()
    return _shared_engine

def get_macro_svc():
    return _shared_macro_svc


# Bot Manager for controlling the lifecycle
class BotManager:
    def __init__(self):
        self.bot = None
        self.thread = None
        self.running = False

    def start(self):
        if self.running:
            return {"message": "Bot is already running"}
        
        config.reload()
        self.bot = LiveBot()
        self.running = True
        self.thread = threading.Thread(target=self._run_bot, daemon=True)
        self.thread.start()
        return {"message": "Bot started successfully"}

    def stop(self):
        if not self.running:
            return {"message": "Bot is not running"}
        
        if self.bot:
            self.bot.stop() # Assuming stop() exists or I'll add it
        self.running = False
        self.thread = None
        return {"message": "Bot stopped successfully"}

    def _run_bot(self):
        try:
            self.bot.start()
        except Exception as e:
            logging.error(f"Bot error: {e}")
        finally:
            self.running = False

bot_manager = BotManager()

# Setup templates and static files
templates = Jinja2Templates(directory=str(config.BASE_DIR / "dashboard" / "templates"))
# app.mount("/static", StaticFiles(directory="dashboard/static"), name="static")

# Shared state file path
STATE_FILE = config.EXE_DIR / "dashboard_state.json"

def get_state():
    state = {
        "status": "Running" if bot_manager.running else "Stopped",
        "balance": 0.0,
        "pnl": 0.0,
        "strategy": "None",
        "symbol": config.ETH_SYMBOL,
        "logs": ["No logs yet."],
        "global_tp_pct": float(getattr(config, "GLOBAL_TP_PCT", 1.0)),
        "exit_profit_pct": float(getattr(config, "EXIT_PROFIT_PCT", 0.5)),
        "exit_strategy_mode": str(getattr(config, "EXIT_STRATEGY_MODE", "OFF"))
    }
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                disk_state = json.load(f)
                state.update(disk_state)
        except:
            pass
    
    # Force status to match bot_manager
    state["status"] = "Running" if bot_manager.running else "Stopped"
    return state

@app.get("/", response_class=HTMLResponse)
async def read_item(request: Request):
    state = get_state()
    return templates.TemplateResponse("index.html", {"request": request, "state": state})

@app.get("/api/state")
def get_state_api():
    from dotenv import dotenv_values
    file_raw_val = "N/A"
    if config.ENV_PATH.exists():
        env_dict = dotenv_values(str(config.ENV_PATH))
        file_raw_val = env_dict.get("GLOBAL_TP_PCT", "NOT FOUND")
        
    logger.info(f"API State: Memory={config.GLOBAL_TP_PCT}, File={file_raw_val}")
    bot_status = "Running" if bot_manager.running else "Stopped"
    logs = ["No logs yet."] 
    
    current_state = get_state()
    if "logs" in current_state:
        logs = current_state["logs"]
    
    engine = get_engine()
    macro_svc = get_macro_svc()
    total_balance = 0.0
    position_data = {"side": "FLAT", "unpnl": 0.0}
    
    try:
        if engine.check_auth():
            total_balance = float(engine.get_total_balance())
            pos = engine.get_position(config.ETH_SYMBOL)
            notional = pos.get("size", 0) * pos.get("entry_price", 0)
            leverage = pos.get("leverage", 1)
            unpnl = pos.get("unrealized_pnl", 0)
            
            roi = 0.0
            if pos.get("side") != "FLAT" and notional > 0:
                margin_used = notional / leverage
                roi = (unpnl / margin_used) * 100
                
            position_data = {
                "side": pos.get("side", "FLAT"),
                "unpnl": round(unpnl, 2),
                "size": pos.get("size", 0),
                "notional": round(notional, 2),
                "roi": round(roi, 2),
                "entry_price": pos.get("entry_price", 0.0),
                "leverage": leverage
            }
    except Exception as e:
        logging.error(f"Execution API error: {e}")
        logs.append(f"API Error: {e}")
    
    # Calculate Market Sentiment (0-100, 50=Neutral)
    sentiment = 50
    try:
        fng = macro_svc.get_fear_and_greed()
        news = macro_svc.get_news_sentiment()
        imb = engine.get_order_book(config.ETH_SYMBOL, limit=5).get("imbalance", {})
        
        fng_val = int(fng.get("value", 50))
        panic = news.get("score", 0)
        bids_pct = imb.get("bids_pct", 50)
        
        # Heuristic: F&G (40%) + Inverse Panic (40%) + Imbalance (20%)
        sentiment = (fng_val * 0.4) + ((100 - panic) * 0.4) + (bids_pct * 0.2)
    except:
        pass
    
    # Change Probability heuristic
    # If LONG and Sentiment is Low -> High chance of change
    # If SHORT and Sentiment is High -> High chance of change
    # If FLAT and Sentiment is extreme -> High chance of entry
    change_prob = 0
    if position_data["side"] == "LONG":
        change_prob = max(0, 100 - sentiment)
    elif position_data["side"] == "SHORT":
        change_prob = max(0, sentiment)
    else: # FLAT
        change_prob = abs(50 - sentiment) * 2
        
    try:
        initial_capital = float(getattr(config, "INITIAL_CAPITAL", 5000))
        yield_pct = ((total_balance - initial_capital) / initial_capital) * 100
    except:
        yield_pct = 0.0
    
    return JSONResponse(content={
        "status": bot_status,
        "logs": logs,
        "balance": round(total_balance, 2),
        "strategy": getattr(config, "ACTIVE_STRATEGY", "N/A"),
        "max_investment": getattr(config, "MAX_INVESTMENT_AMOUNT", 0.0),
        "margin_mode": "ISOLATED" if getattr(config, "ISOLATED_MARGIN", True) else "CROSS",
        "yield_pct": round(yield_pct, 2),
        "position": position_data,
        "sentiment": round(sentiment, 1),
        "change_prob": round(change_prob, 1),
        "exit_strategy_mode": str(getattr(config, "EXIT_STRATEGY_MODE", "OFF")),
        "exit_profit_pct": float(getattr(config, "EXIT_PROFIT_PCT", 0.5)),
        "global_tp_pct": float(getattr(config, "GLOBAL_TP_PCT", 1.0)),
        "debug_env_value": file_raw_val
    }, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.post("/api/bot/start")
async def start_bot():
    return bot_manager.start()

@app.post("/api/bot/stop")
async def stop_bot():
    return bot_manager.stop()

@app.post("/api/bot/force_exit")
async def force_exit():
    config.reload()
    engine = get_engine()
    result = engine.close_position(config.ETH_SYMBOL)
    if result:
        # Send Telegram notification for manual force exit
        try:
            from telegram_notifier import TelegramNotifier
            tg = TelegramNotifier(
                token=getattr(config, 'TELEGRAM_BOT_TOKEN', ''),
                chat_id=getattr(config, 'TELEGRAM_CHAT_ID', '')
            )
            tg.send(f"⚠️ *MANUAL FORCE EXIT*\nSymbol: `{config.ETH_SYMBOL}`\nPosition Closed Successfully.")
        except Exception as e:
            logging.error(f"Failed to send force exit TG notification: {e}")
            
        return {"message": "Force exit successful", "order": str(result)}
    return {"error": "Force exit failed"}

@app.get("/api/orderbook")
def get_orderbook():
    engine = get_engine()
    return engine.get_order_book(config.ETH_SYMBOL, limit=10)

@app.get("/api/config")
def get_config():
    from dotenv import dotenv_values
    config_vars = {}
    if config.ENV_PATH.exists():
        # Use dotenv_values for robust parsing with absolute path
        raw_vars = dotenv_values(str(config.ENV_PATH))
        for key, value in raw_vars.items():
            # Don't send sensitive keys to the UI
            if ("SECRET" in key or "KEY" in key) and "KEYWORDS" not in key:
                config_vars[key] = "********"
            else:
                config_vars[key] = value
    
    return JSONResponse(content=config_vars, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.post("/api/config")
async def update_config(data: dict):
    if not config.ENV_PATH.exists():
        return {"error": ".env file not found at " + str(config.ENV_PATH)}
    
    from dotenv import dotenv_values, set_key
    # Read existing vars to preserve comments or structure if needed, or just use set_key
    # set_key is the most reliable way to update specific keys in .env
    for key, value in data.items():
        if value != "********":
            set_key(str(config.ENV_PATH), key, str(value))
            logger.info(f"Updated config: {key} = {value}")
    
    config.reload()
    global _shared_engine
    _shared_engine = None  # Force re-init after config update
    return {"message": "Config updated successfully"}

@app.get("/api/market")
def get_market(interval: str = "24h"):
    engine = get_engine()
    data = engine.get_market_data(config.ETH_SYMBOL, interval=interval)
    return data or {"error": "Could not fetch market data"}

@app.get("/api/trades")
def get_trades():
    engine = get_engine()
    trades = engine.get_trade_history(config.ETH_SYMBOL)
    return trades

@app.get("/api/history")
def get_history(timeframe: str = None, limit: int = 100):
    tf = timeframe or config.ETH_TIMEFRAME
    engine = get_engine()
    return engine.get_ohlcv(config.ETH_SYMBOL, tf, limit)

@app.get("/api/macro")
async def get_macro_data():
    import asyncio
    engine = get_engine()
    macro_svc = get_macro_svc()
    
    loop = asyncio.get_event_loop()
    
    # Run synchronous network calls in executor
    futures_metrics = await loop.run_in_executor(None, engine.get_futures_metrics, config.ETH_SYMBOL)
    sentiment = await loop.run_in_executor(None, macro_svc.get_fear_and_greed)
    indices = await loop.run_in_executor(None, macro_svc.get_macro_indices)
    news_risk = await loop.run_in_executor(None, macro_svc.get_news_sentiment)
    gold_corr = await loop.run_in_executor(None, macro_svc.get_gold_correlation)
    social_vol = await loop.run_in_executor(None, macro_svc.get_social_volume)
    pos_momentum = await loop.run_in_executor(None, macro_svc.get_positive_momentum)
    
    return {
        "futures": futures_metrics,
        "sentiment": sentiment,
        "macro": indices,
        "risk": {
            "news_score": news_risk.get("score"),
            "news_hits": news_risk.get("keyword_hits"),
            "news_sta": news_risk.get("sta_hits", 0),
            "news_lta": news_risk.get("lta_hits", 0),
            "news_details": news_risk.get("details", {}),
            "gold_correlation": gold_corr.get("correlation"),
            "gold_details": gold_corr.get("details", {}),
            "social_trends": social_vol,
            "positive_momentum": pos_momentum
        }
    }

@app.get("/api/config/current_symbol")
async def get_current_symbol():
    config.reload()
    return {"symbol": config.ETH_SYMBOL}

@app.get("/api/config/env_status")
async def get_env_status():
    config.reload()
    return {
        "use_testnet": config.USE_TESTNET,
        "mode": config.ETH_MODE
    }

@app.post("/api/config/environment")
async def toggle_environment(data: dict):
    use_testnet = data.get("use_testnet", True)
    
    # Update .env file - use root path
    env_path = ".env"
    if not os.path.exists(env_path):
        return {"error": ".env file not found"}

    with open(env_path, "r") as f:
        lines = f.readlines()
    
    found = False
    new_lines = []
    for line in lines:
        if line.strip().startswith("USE_TESTNET="):
            new_lines.append(f"USE_TESTNET={'1' if use_testnet else '0'}\n")
            found = True
        else:
            new_lines.append(line)
    
    if not found:
        new_lines.append(f"USE_TESTNET={'1' if use_testnet else '0'}\n")
            
    with open(env_path, "w") as f:
        f.writelines(new_lines)
    
    # Also update ETH_MODE for consistency
    await update_config({"ETH_MODE": "TESTNET" if use_testnet else "LIVE"})
    
    config.reload()
    return {"message": f"Environment switched to {'TESTNET' if use_testnet else 'MAINNET'}"}

@app.get("/api/symbols")
def get_symbols():
    engine = get_engine()
    return engine.get_available_symbols()

@app.post("/api/config/symbol")
async def update_symbol(data: dict):
    symbol = data.get("symbol")
    if not symbol: return {"error": "No symbol provided"}
    
    # Update .env
    return await update_config({"ETH_SYMBOL": symbol})

if __name__ == "__main__":
    import uvicorn
    # Use config.IS_FROZEN to toggle reload mode
    # Reload mode must be OFF in a bundled EXE
    is_reload = not config.IS_FROZEN
    
    if config.IS_FROZEN:
        # Standard execution for EXE
        logger.info("Running in FROZEN EXE mode. Reloader disabled for stability.")
        uvicorn.run(app, host="0.0.0.0", port=8000, reload=False, workers=1)
    else:
        # Development mode
        logger.info("Running in DEV mode. Reloader enabled.")
        uvicorn.run("dashboard_app:app", host="0.0.0.0", port=8000, reload=True)
