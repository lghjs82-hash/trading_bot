import requests
import threading
import logging
import os
import config

logger = logging.getLogger("TelegramBotHandler")

# ─── Helpers ────────────────────────────────────────────────────────────────

STRATEGIES = {
    "ema": "EMACrossover",
    "emacrossover": "EMACrossover",
    "macd": "MACDTrend",
    "macdtrend": "MACDTrend",
    "rsi": "RSIReversion",
    "rsireversion": "RSIReversion",
    "ss": "StructureShock",
    "structureshock": "StructureShock",
    "custom": "CustomStrategy",
}

COINS = {
    "eth": ("ETH/USDT:USDT", "ETHUSDT"),
    "ethereum": ("ETH/USDT:USDT", "ETHUSDT"),
    "btc": ("BTC/USDT:USDT", "BTCUSDT"),
    "bitcoin": ("BTC/USDT:USDT", "BTCUSDT"),
}

HELP_TEXT = (
    "🤖 *Trading Bot 명령어 목록*\n"
    "━━━━━━━━━━━━━━\n"
    "▶️ *봇 제어*\n"
    "`/start_bot` — 봇 시작\n"
    "`/stop_bot` — 봇 정지\n\n"
    "⚙️ *설정 변경*\n"
    "`/mode testnet` — 테스트넷 전환\n"
    "`/mode mainnet` — 상용 전환\n"
    "`/strategy ema` — EMA 전략\n"
    "`/strategy macd` — MACD 전략\n"
    "`/strategy rsi` — RSI 전략\n"
    "`/strategy ss` — StructureShock 전략\n"
    "`/coin eth` — 이더리움으로 변경\n"
    "`/coin btc` — 비트코인으로 변경\n\n"
    "📊 *조회*\n"
    "`/status` — 봇 현재 상태\n"
    "`/balance` — 잔고 조회\n"
    "`/position` — 포지션 조회\n"
    "`/market` — 시장 현황\n"
    "`/sentiment` — 시장 심리 지표\n"
    "`/help` — 이 도움말"
)


def _update_env(key: str, value: str):
    """Update a single key in the .env file."""
    env_path = ".env"
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env_vars[k] = v
    env_vars[key] = value
    with open(env_path, "w") as f:
        for k, v in env_vars.items():
            f.write(f"{k}={v}\n")
    config.reload()


# ─── Handler ──────────────────────────────────────────────────────────────────

class TelegramBotHandler:
    def __init__(self, token: str, chat_id: str, bot_manager):
        self.token = token
        self.chat_id = str(chat_id)
        self.bot_manager = bot_manager
        self.enabled = bool(token and chat_id)
        self._running = False
        self._offset = 0
        self._base_url = f"https://api.telegram.org/bot{token}"

    # ── Messaging ──

    def send(self, text: str):
        """Send a message to the configured chat. Logs API errors for visibility."""
        if not self.enabled:
            return
        try:
            # Pre-sanitize: Minimal escaping for HTML mode which is more robust than Markdown for dynamic text
            safe_text = text.replace("<", "&lt;").replace(">", "&gt;")
            
            resp = requests.post(
                f"{self._base_url}/sendMessage",
                json={"chat_id": self.chat_id, "text": safe_text, "parse_mode": "HTML"},
                timeout=10
            )
            if resp.status_code != 200:
                logger.error(f"Telegram API Error ({resp.status_code}): {resp.text}")
        except Exception as e:
            logger.warning(f"Failed to send Telegram message: {e}")

    def _get_updates(self):
        """Fetch new messages from Telegram with a longer timeout for polling."""
        try:
            resp = requests.get(
                f"{self._base_url}/getUpdates",
                params={"offset": self._offset, "timeout": 30},
                timeout=35
            )
            if resp.status_code == 200:
                return resp.json().get("result", [])
            else:
                logger.error(f"Telegram getUpdates failed ({resp.status_code}): {resp.text}")
        except Exception as e:
            logger.warning(f"Telegram connection error: {e}")
        return []

    # ── Command Handlers ──

    def _cmd_status(self):
        status = "<b>🟢 Running</b>" if self.bot_manager.running else "<b>🔴 Stopped</b>"
        mode = getattr(config, "ETH_MODE", "N/A")
        mode_tag = "🧪 TESTNET" if mode == "TESTNET" else "💰 LIVE"
        self.send(
            f"📊 <b>Bot Status</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🔘 Status: {status}\n"
            f"📌 Symbol: <code>{getattr(config, 'ETH_SYMBOL', 'N/A')}</code>\n"
            f"📈 Strategy: <code>{getattr(config, 'ACTIVE_STRATEGY', 'N/A')}</code>\n"
            f"🏷️ Mode: {mode_tag}"
        )

    def _cmd_balance(self):
        try:
            from execution_engine import ExecutionEngine
            engine = ExecutionEngine()
            balance = engine.get_total_balance()
            initial = float(getattr(config, "INITIAL_CAPITAL", 5000))
            pnl_pct = ((balance - initial) / initial * 100) if initial > 0 else 0
            pnl_sign = "+" if pnl_pct >= 0 else ""
            
            # Position info for actual investment metrics
            pos = engine.get_position(config.ETH_SYMBOL)
            side = pos.get("side", "FLAT")
            
            notional = 0
            roi = 0
            if side != "FLAT":
                notional = pos["size"] * pos["entry_price"]
                margin = notional / pos["leverage"] if pos["leverage"] > 0 else notional
                roi = (pos["unrealized_pnl"] / margin * 100) if margin > 0 else 0
            
            roi_sign = "+" if roi >= 0 else ""

            self.send(
                f"⚡️ <b>Version: 1.3</b>\n"
                f"💰 <b>Balance Status</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"💵 Total: <code>{round(balance, 2)} USDT</code>\n"
                f"🏦 Initial: <code>{initial} USDT</code>\n"
                f"📈 Total ROI: <b>{pnl_sign}{round(pnl_pct, 2)}%</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"🛡️ Invested: <code>{round(notional, 2)} USDT</code>\n"
                f"📊 Actual ROI: <b>{roi_sign}{round(roi, 2)}%</b>"
            )
        except Exception as e:
            self.send(f"❌ 잔고 조회 실패: `{e}`")

    def _cmd_position(self):
        try:
            from execution_engine import ExecutionEngine
            engine = ExecutionEngine()
            pos = engine.get_position(getattr(config, "ETH_SYMBOL", "ETH/USDT:USDT"))
            side = pos.get("side", "FLAT")
            side_emoji = "🟢" if side == "LONG" else ("🔴" if side == "SHORT" else "⚪")
            
            notional = 0
            roi = 0
            if side != "FLAT":
                notional = pos["size"] * pos["entry_price"]
                margin = notional / pos["leverage"] if pos["leverage"] > 0 else notional
                roi = (pos["unrealized_pnl"] / margin * 100) if margin > 0 else 0
            
            roi_sign = "+" if roi >= 0 else ""

            msg = (
                f"⚡️ <b>Version: 1.3</b>\n"
                f"{side_emoji} <b>Position Metrics</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"📌 Symbol: <code>{getattr(config, 'ETH_SYMBOL', 'N/A')}</code>\n"
                f"🔘 Side: <b>{side}</b>\n"
                f"📊 Size: <code>{pos.get('size', 0)}</code>\n"
                f"💵 Entry: <code>{pos.get('entry_price', 0)}</code>\n"
                f"🛡️ Invested: <code>{round(notional, 2)} USDT</code>\n"
                f"💰 Unrealized: <code>{round(pos.get('unrealized_pnl', 0), 2)} USDT</code>\n"
                f"📊 Actual ROI: <b>{roi_sign}{round(roi, 2)}%</b>"
            )
            
            self.send(msg)
        except Exception as e:
            self.send(f"❌ 포지션 조회 실패: `{e}`")

    def _cmd_market(self):
        try:
            resp = requests.get("http://localhost:8000/api/market?interval=24h", timeout=5)
            d = resp.json()
            self.send(
                f"📊 <b>Market (24h)</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"📌 Symbol: <code>{getattr(config, 'ETH_SYMBOL', 'N/A')}</code>\n"
                f"💵 Price: <code>{round(d.get('last', 0), 2)} USDT</code>\n"
                f"📈 Change: <b>{round(d.get('percentage', 0), 2)}%</b>\n"
                f"🔺 High: <code>{round(d.get('high', 0), 2)}</code>\n"
                f"🔻 Low: <code>{round(d.get('low', 0), 2)}</code>\n"
                f"📦 Volume: <code>{round(d.get('volume', 0)):,}</code>"
            )
        except Exception as e:
            self.send(f"❌ 시장 조회 실패: `{e}`")

    def _cmd_sentiment(self):
        try:
            resp = requests.get("http://localhost:8000/api/state", timeout=5)
            d = resp.json()
            sentiment = d.get("sentiment", 50)
            change_prob = d.get("change_prob", 0)
            sent_label = "😱 극단적 공포" if sentiment < 20 else ("😨 공포" if sentiment < 40 else ("😐 중립" if sentiment < 60 else ("😊 탐욕" if sentiment < 80 else "🤑 극단적 탐욕")))
            self.send(
                f"🧠 <b>Market Sentiment</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"❤️ Sentiment: <b>{sentiment}</b> — {sent_label}\n"
                f"🎯 Change Prob: <code>{change_prob}%</code>\n"
                f"📌 Position: <code>{d.get('position', {}).get('side', 'FLAT')}</code>"
            )
        except Exception as e:
            self.send(f"❌ 심리 조회 실패: `{e}`")

    def _cmd_mode(self, arg: str):
        if arg == "testnet":
            _update_env("ETH_MODE", "TESTNET")
            self.send("✅ <b>테스트넷으로 전환되었습니다!</b> 🧪\n봇을 재시작하면 적용됩니다.")
        elif arg == "mainnet":
            _update_env("ETH_MODE", "MAINNET")
            self.send("✅ <b>상용 모드로 전환되었습니다!</b> 💰\n봇을 재시작하면 적용됩니다.")
        else:
            self.send("❌ 올바른 모드: `/mode testnet` 또는 `/mode mainnet`")

    def _cmd_strategy(self, arg: str):
        strategy = STRATEGIES.get(arg.lower())
        if strategy:
            _update_env("ACTIVE_STRATEGY", strategy)
            self.send(f"✅ *전략이 `{strategy}`으로 변경되었습니다!*")
        else:
            opts = ", ".join([f"`{k}`" for k in ["ema", "macd", "rsi", "ss", "custom"]])
            self.send(f"❌ 올바른 전략: {opts}")

    def _cmd_coin(self, arg: str):
        coin = COINS.get(arg.lower())
        if coin:
            symbol, pair = coin
            _update_env("ETH_SYMBOL", symbol)
            self.send(f"✅ *코인이 `{symbol}`으로 변경되었습니다!*\n봇 재시작 후 적용됩니다.")
        else:
            self.send("❌ 올바른 코인: `/coin eth` 또는 `/coin btc`")

    # ── Update Router ──

    def _handle_update(self, update):
        message = update.get("message", {})
        chat_id = str(message.get("chat", {}).get("id", ""))
        text = (message.get("text") or "").strip()

        if chat_id != self.chat_id:
            logger.warning(f"Unauthorized Telegram access from chat_id={chat_id}")
            return

        if not text.startswith("/"):
            return

        # Improved Command Extraction: Handle /cmd@BotName or just /cmd
        parts = text.split(maxsplit=1)
        full_cmd = parts[0].lower().lstrip("/")
        cmd = full_cmd.split("@")[0] # Extract 'start_bot' from 'start_bot@MyBot'
        
        arg = parts[1].strip().lower() if len(parts) > 1 else ""

        if cmd in ["start_bot", "start_bot"]:
            result = self.bot_manager.start()
            self.send(f"🟢 <b>{result.get('message', 'Bot started')}</b>")
        elif cmd in ["stop_bot", "stop_bot"]:
            result = self.bot_manager.stop()
            self.send(f"🔴 <b>{result.get('message', 'Bot stopped')}</b>")
        elif cmd == "status":
            self._cmd_status()
        elif cmd == "balance":
            self._cmd_balance()
        elif cmd == "position":
            self._cmd_position()
        elif cmd == "market":
            self._cmd_market()
        elif cmd == "sentiment":
            self._cmd_sentiment()
        elif cmd == "mode":
            self._cmd_mode(arg)
        elif cmd == "strategy":
            self._cmd_strategy(arg)
        elif cmd == "coin":
            self._cmd_coin(arg)
        elif cmd in ["help", "start"]:
            self.send(HELP_TEXT)
        else:
            self.send(f"❓ 알 수 없는 명령어: <code>{text}</code>\n<code>/help</code> 를 입력하면 사용 가능한 명령어를 확인할 수 있습니다.")

    # ── Polling Loop ──

    def _poll_loop(self):
        """Main Polling Loop with Supervisor for Resilience"""
        logger.info("Telegram command handler (supervisor) started.")
        self._running = True
        
        # Initial greeting indicating the daemon is alive
        self.send(
            "🤖 <b>Bot Commander Ready</b>\n"
            "━━━━━━━━━━━━━━\n"
            "I'm now monitoring commands. Type <code>/help</code> for list."
        )

        error_wait = 5 # Initial wait on error
        
        while self._running:
            try:
                updates = self._get_updates()
                error_wait = 5 # Reset wait on success
                
                for update in updates:
                    self._offset = update["update_id"] + 1
                    try:
                        self._handle_update(update)
                    except Exception as e:
                        logger.error(f"Error handling Telegram command: {e}")
                        
            except Exception as e:
                import time
                wait_time = min(60, error_wait * 2)
                logger.error(f"Telegram Polling Error: {e}. Retrying in {wait_time}s...")
                time.sleep(error_wait)
                error_wait = wait_time
        
        logger.info("Telegram command handler stopped.")

    def start(self):
        if not self.enabled:
            logger.info("Telegram handler disabled (no token/chat_id).")
            return
        # Start as a daemon thread so it doesn't block exit
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def stop(self):
        self._running = False
