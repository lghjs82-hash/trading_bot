import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Base Directory for absolute path resolution
# When running as an EXE (frozen), _MEIPASS is the extraction folder for assets.
# But for the .env, we want the folder where the actual EXE is located.
IS_FROZEN = getattr(sys, 'frozen', False)
if IS_FROZEN:
    # Directory where the executable is located
    EXE_DIR = Path(sys.executable).resolve().parent
    # Directory where assets are bundled (temporary folder)
    BASE_DIR = Path(sys._MEIPASS).resolve()
else:
    EXE_DIR = Path(__file__).resolve().parent
    BASE_DIR = EXE_DIR

ENV_PATH = EXE_DIR / ".env"

# Configure logging for config module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Config")

def setup_wizard():
    """Prompt user for missing mandatory environment variables if .env is missing"""
    if not ENV_PATH.exists():
        print("\n" + "="*50)
        print("首次运行: 트레이딩 봇 초기 설정 (First Run Setup)")
        print("="*50)
        print(".env 파일을 찾을 수 없습니다. API 키를 입력해 주세요.")
        print("(Press Enter to skip/use default)\n")
        
        testnet_key = input("Binance TESTNET API Key: ").strip()
        testnet_secret = input("Binance TESTNET API Secret: ").strip()
        mainnet_key = input("Binance MAINNET API Key: ").strip()
        mainnet_secret = input("Binance MAINNET API Secret: ").strip()
        
        with open(ENV_PATH, "w", encoding='utf-8') as f:
            f.write(f"TESTNET_API_KEY={testnet_key}\n")
            f.write(f"TESTNET_API_SECRET={testnet_secret}\n")
            f.write(f"MAINNET_API_KEY={mainnet_key}\n")
            f.write(f"MAINNET_API_SECRET={mainnet_secret}\n")
            f.write("USE_TESTNET=1\n")
            f.write("ETH_SYMBOL=BTC/USDT:USDT\n")
            f.write("ACTIVE_STRATEGY=MultiFilterMomentum\n")
        
        print("\n설정이 완료되었습니다! (Setup Complete!)")
        print(f"설정 파일 저장 위치: {ENV_PATH}\n")

def get_env_str(name: str, default: str, env_data: dict = None) -> str:
    v = (env_data.get(name) if env_data else None) or os.getenv(name)
    return default if v is None or str(v).strip() == "" else str(v).strip()

def get_env_int(name: str, default: int, env_data: dict = None) -> int:
    v = (env_data.get(name) if env_data else None) or os.getenv(name)
    if v is None or str(v).strip() == "": return int(default)
    try: return int(float(str(v).strip()))
    except: return int(default)

def get_env_float(name: str, default: float, env_data: dict = None) -> float:
    v = (env_data.get(name) if env_data else None) or os.getenv(name)
    if v is None or str(v).strip() == "": return float(default)
    try: return float(str(v).strip())
    except: return float(default)

def get_env_bool(name: str, default: bool, env_data: dict = None) -> bool:
    v = (env_data.get(name) if env_data else None) or os.getenv(name)
    if v is None or str(v).strip() == "": return bool(default)
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

class _Config:
    def __init__(self):
        self._last_mtime = 0
        self.reload(force=True)

    def reload(self, force=False):
        from dotenv import dotenv_values
        
        # Run wizard if needed
        if not ENV_PATH.exists():
            setup_wizard()

        # Caching logic: check mtime of .env
        try:
            current_mtime = os.path.getmtime(str(ENV_PATH))
            if not force and current_mtime <= self._last_mtime:
                # No change since last load, skip disk read
                return
            self._last_mtime = current_mtime
        except Exception as e:
            # If error getting mtime, proceed with reload anyway
            pass

        print(f"\n[DEBUG] --- Config Reload Start (mtime changed) ---")
        print(f"[DEBUG] Looking for .env at: {ENV_PATH}")
        
        env_data = {}
        if ENV_PATH.exists():
            load_dotenv(dotenv_path=str(ENV_PATH), override=True)
            env_data = dotenv_values(str(ENV_PATH))
            print(f"[DEBUG] Raw GLOBAL_TP_PCT from .env file: {env_data.get('GLOBAL_TP_PCT', 'NOT FOUND')}")
        else:
            print(f"[DEBUG] WARNING: .env file NOT FOUND at {ENV_PATH}")
            
        self.USE_TESTNET = get_env_bool("USE_TESTNET", True, env_data)
        self.MAINNET_API_KEY = get_env_str("MAINNET_API_KEY", "", env_data)
        self.MAINNET_API_SECRET = get_env_str("MAINNET_API_SECRET", "", env_data)
        self.TESTNET_API_KEY = get_env_str("TESTNET_API_KEY", "", env_data)
        self.TESTNET_API_SECRET = get_env_str("TESTNET_API_SECRET", "", env_data)
        
        if self.USE_TESTNET:
            self.BINANCE_API_KEY = self.TESTNET_API_KEY
            self.BINANCE_API_SECRET = self.TESTNET_API_SECRET
            self.ETH_MODE = "TESTNET"
        else:
            self.BINANCE_API_KEY = self.MAINNET_API_KEY
            self.BINANCE_API_SECRET = self.MAINNET_API_SECRET
            self.ETH_MODE = "LIVE"

        self.BOT_NAME = get_env_str("BOT_NAME", "Binance Trading Bot", env_data)
        self.ETH_SYMBOL = get_env_str("ETH_SYMBOL", "ETH/USDT:USDT", env_data)
        self.ETH_TIMEFRAME = get_env_str("ETH_TIMEFRAME", "5m", env_data)
        self.INITIAL_CAPITAL = get_env_float("INITIAL_CAPITAL", 1000.0, env_data)
        self.RISK_PER_TRADE_PCT = get_env_float("RISK_PER_TRADE_PCT", 0.02, env_data)
        self.FEE_RATE = get_env_float("FEE_RATE", 0.001, env_data)
        self.SLIPPAGE_RATE = get_env_float("SLIPPAGE_RATE", 0.001, env_data)
        
        self.ACTIVE_STRATEGY = get_env_str("ACTIVE_STRATEGY", "GoldenCross", env_data)
        self.EMA_FAST_LEN = get_env_int("EMA_FAST_LEN", 12, env_data)
        self.EMA_SLOW_LEN = get_env_int("EMA_SLOW_LEN", 26, env_data)
        self.RSI_LEN = get_env_int("RSI_LEN", 14, env_data)
        self.RSI_OVERBOUGHT = get_env_float("RSI_OVERBOUGHT", 70.0, env_data)
        self.RSI_OVERSOLD = get_env_float("RSI_OVERSOLD", 30.0, env_data)
        self.MACD_FAST = get_env_int("MACD_FAST", 12, env_data)
        self.MACD_SLOW = get_env_int("MACD_SLOW", 26, env_data)
        self.MACD_SIGNAL = get_env_int("MACD_SIGNAL", 9, env_data)
        self.CUSTOM_PARAM_1 = get_env_float("CUSTOM_PARAM_1", 1.5, env_data)
        self.CUSTOM_PARAM_2 = get_env_float("CUSTOM_PARAM_2", 80.0, env_data)
        
        self.ALLOW_LONG = get_env_bool("ALLOW_LONG", True, env_data)
        self.ALLOW_SHORT = get_env_bool("ALLOW_SHORT", True, env_data)
        self.LONG_ENGINE_ENABLED = get_env_bool("LONG_ENGINE_ENABLED", True, env_data)
        self.ISOLATED_MARGIN = get_env_bool("ISOLATED_MARGIN", True, env_data)
        
        self.NEGATIVE_NEWS_KEYWORDS = get_env_str("NEGATIVE_NEWS_KEYWORDS", "war, conflict, pandemic", env_data)
        self.POSITIVE_NEWS_KEYWORDS = get_env_str("POSITIVE_NEWS_KEYWORDS", "adoption, ETF", env_data)
        self.TELEGRAM_BOT_TOKEN = get_env_str("TELEGRAM_BOT_TOKEN", "", env_data)
        self.TELEGRAM_CHAT_ID = get_env_str("TELEGRAM_CHAT_ID", "", env_data)
        self.SAFETY_GUARD_THRESHOLD = get_env_int("SAFETY_GUARD_THRESHOLD", 101, env_data)
        self.MAX_INVESTMENT_AMOUNT = get_env_float("MAX_INVESTMENT_AMOUNT", 100.0, env_data)
        self.ALLOW_MULTIPLE_ENTRIES = get_env_bool("ALLOW_MULTIPLE_ENTRIES", False, env_data)
        self.MAX_ENTRIES_COUNT = get_env_int("MAX_ENTRIES_COUNT", 3, env_data)
        self.EXIT_STRATEGY_MODE = get_env_str("EXIT_STRATEGY_MODE", "OFF", env_data)
        self.EXIT_PROFIT_PCT = get_env_float("EXIT_PROFIT_PCT", 0.5, env_data)
        self.GLOBAL_TP_PCT = get_env_float("GLOBAL_TP_PCT", 1.0, env_data)
        
        print(f"[DEBUG] Final Memory GLOBAL_TP_PCT: {self.GLOBAL_TP_PCT}")
        
        # Explicitly broadcast to module level
        module_dict = globals()
        for key, value in self.__dict__.items():
            if not key.startswith('_'): # Don't export private members
                module_dict[key] = value
        print(f"[DEBUG] --- Config Reload End ---\n")

_c = _Config()

def reload(force=False):
    _c.reload(force=force)
    module_dict = globals()
    for key, value in _c.__dict__.items():
        if not key.startswith('_'):
            module_dict[key] = value

# Initial sync
reload(force=True)
