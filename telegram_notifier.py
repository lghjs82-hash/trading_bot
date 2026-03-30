import requests
import logging

logger = logging.getLogger("TelegramNotifier")

class TelegramNotifier:
    """Sends trading alert messages to a Telegram chat."""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id and token != "your_telegram_bot_token")
    
    def send(self, message: str):
        """Send a message to the configured Telegram chat."""
        if not self.enabled:
            logger.debug("Telegram notifier not configured, skipping.")
            return False
        
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                logger.info("Telegram notification sent.")
                return True
            else:
                logger.warning(f"Telegram API error {resp.status_code}: {resp.text}")
                return False
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False

    def notify_order(self, side: str, symbol: str, amount: float, price: float, order_id: str, mode: str = "TESTNET"):
        """Send a formatted trade execution notification."""
        emoji = "🟢 LONG" if side == "LONG" else "🔴 SHORT"
        mode_tag = "🧪 TESTNET" if mode == "TESTNET" else "💰 LIVE"
        message = (
            f"*{emoji} Order Executed!*\n"
            f"━━━━━━━━━━━━━━\n"
            f"📌 Symbol: `{symbol}`\n"
            f"📊 Amount: `{amount}`\n"
            f"💵 Price: `{price}`\n"
            f"🔖 Order ID: `{order_id}`\n"
            f"🏷️ Mode: {mode_tag}"
        )
        return self.send(message)

    def notify_error(self, symbol: str, error: str):
        """Send an error notification."""
        message = (
            f"❌ *Order Failed*\n"
            f"━━━━━━━━━━━━━━\n"
            f"📌 Symbol: `{symbol}`\n"
            f"⚠️ Error: `{error}`"
        )
        return self.send(message)
