import threading
import time
import uvicorn
from live_bot import LiveBot
from dashboard_app import app, bot_manager
from telegram_bot_handler import TelegramBotHandler
import config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_dashboard():
    # Enable reload to pick up code changes in dashboard_app.py
    uvicorn.run("dashboard_app:app", host="0.0.0.0", port=8000, reload=False)

if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 Trading Bot Dashboard started!")
    print("👉 View at: http://localhost:8000")
    print("="*50 + "\n")

    # Start Telegram command handler in background thread
    config.reload()
    tg_token = getattr(config, 'TELEGRAM_BOT_TOKEN', '')
    tg_chat_id = getattr(config, 'TELEGRAM_CHAT_ID', '')
    
    tg_handler = TelegramBotHandler(
        token=tg_token,
        chat_id=tg_chat_id,
        bot_manager=bot_manager
    )
    tg_handler.start()
    
    if tg_token:
        print("📨 Telegram command handler started!")
        print(f"   Send /help to your bot to see available commands.\n")
    
    # Run dashboard in the main thread
    run_dashboard()
