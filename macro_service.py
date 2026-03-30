import yfinance as yf
import requests
import logging
import pandas as pd
import config

logger = logging.getLogger("MacroService")

class MacroService:
    def __init__(self):
        # Alternative.me Fear and Greed API
        self.fng_url = "https://api.alternative.me/fng/"
        import time
        self._cache = {}
        
    def _get_cached(self, key, ttl_seconds, fetch_func):
        import time
        now = time.time()
        if key in self._cache:
            val, entry_time = self._cache[key]
            if now - entry_time < ttl_seconds:
                return val
        
        val = fetch_func()
        self._cache[key] = (val, now)
        return val
    
    def get_fear_and_greed(self):
        return self._get_cached("fng", 60, self._fetch_fear_and_greed)
        
    def _fetch_fear_and_greed(self):
        """Fetch current crypto Fear and Greed Index"""
        try:
            resp = requests.get(self.fng_url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if "data" in data and len(data["data"]) > 0:
                    item = data["data"][0]
                    return {
                        "value": int(item["value"]),
                        "classification": item["value_classification"]
                    }
            return {"value": 50, "classification": "Neutral"}
        except Exception as e:
            logger.warning(f"Error fetching F&G index: {e}")
            return {"value": 0, "classification": "Error"}

    def get_macro_indices(self):
        return self._get_cached("macro_idx", 60, self._fetch_macro_indices)

    def _fetch_macro_indices(self):
        """Fetch DXY and NASDAQ futures via yfinance"""
        try:
            tickers = yf.Tickers("DX-Y.NYB NQ=F")
            dxy_info = tickers.tickers["DX-Y.NYB"].fast_info
            nq_info = tickers.tickers["NQ=F"].fast_info
            
            return {
                "DXY": {
                    "price": round(dxy_info.get("lastPrice", 0), 2),
                },
                "NASDAQ": {
                    "price": round(nq_info.get("lastPrice", 0), 2),
                }
            }
        except Exception as e:
            logger.warning(f"Error fetching macro indices: {e}")
            return {"DXY": {"price": 0}, "NASDAQ": {"price": 0}}
            
    def get_news_sentiment(self):
        return self._get_cached("news", 600, self._fetch_news_sentiment)
        
    def _fetch_news_sentiment(self):
        """Fetch RSS crypto/world news to calculate a 0-100 panic score based on keywords"""
        import feedparser
        import urllib.parse
        try:
            # Use keywords from config, fallback to defaults if empty
            neg_keywords = config.NEGATIVE_NEWS_KEYWORDS
            if not neg_keywords:
                neg_keywords = "war, conflict, pandemic, recession, crisis, lockdown"
            
            # Convert comma-separated string to OR query: (word1 OR word2 OR ...)
            keywords_list = [k.strip() for k in neg_keywords.split(",") if k.strip()]
            q = f"({' OR '.join(keywords_list)})"
            
            # Fetch 24h (Short-Term)
            url_24h = f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}+when:24h&hl=en-US&gl=US&ceid=US:en"
            feed_24h = feedparser.parse(url_24h)
            hits = len(feed_24h.entries) # sta_hits
            
            # Fetch 7d (Long-Term Baseline)
            url_7d = f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}+when:7d&hl=en-US&gl=US&ceid=US:en"
            feed_7d = feedparser.parse(url_7d)
            lta = len(feed_7d.entries) / 7.0  # daily average over 7 days
            
            # Bug Fix: Use RELATIVE spike score, not absolute count
            # Score = how much today's news EXCEEDS the weekly average
            # e.g. if weekly avg is 40/day and today is 80 → spike ratio = 2.0 → score = 80
            # This prevents always-on safety guard in high-noise environments
            if lta > 0:
                spike_ratio = hits / lta  # ratio of today vs baseline
                # Only score HIGH if there's an actual spike (>2x normal)
                if spike_ratio >= 2.0:
                    score = min(int((spike_ratio - 1.0) * 50), 100)
                else:
                    score = 0  # Normal news level, no panic
            else:
                # No 7d baseline yet: use absolute but with safer cap
                score = min(hits, 60)  # max 60 if no baseline
            
            # Extract top 5 headlines for the details modal
            headlines = [{"title": entry.title, "link": entry.link} for entry in feed_24h.entries[:5]]
            
            return {
                "score": score, 
                "keyword_hits": hits,
                "sta_hits": hits,
                "lta_hits": round(lta, 1),
                "details": {"headlines": headlines, "description": "Relative spike score: compares 24h keyword hits vs 7-day daily average. Score >80 triggers Safety Guard only on genuine news spikes."}
            }
        except Exception as e:
            logger.warning(f"Error fetching news sentiment: {e}")
            return {"score": 0, "keyword_hits": 0, "sta_hits": 0, "lta_hits": 0, "details": {}}

    def get_positive_momentum(self):
        return self._get_cached("momentum", 600, self._fetch_positive_momentum)
        
    def _fetch_positive_momentum(self):
        """Golden Cross logic: compares 24h vs 7d mentions of positive keywords"""
        import feedparser
        import urllib.parse
        try:
            # Use keywords from config, fallback to defaults if empty
            pos_keywords = config.POSITIVE_NEWS_KEYWORDS
            if not pos_keywords:
                pos_keywords = "ETF, Institutional, Fed, Halving, BlackRock, Mainnet, Layer 2"
            
            # Convert comma-separated string to OR query: (word1 OR word2 OR ...)
            keywords_list = [k.strip() for k in pos_keywords.split(",") if k.strip()]
            keywords = f"({' OR '.join(keywords_list)})"
            
            # Fetch 24h (Short-Term Surge)
            url_24h = f"https://news.google.com/rss/search?q={urllib.parse.quote(keywords)}+when:24h&hl=en-US&gl=US&ceid=US:en"
            feed_24h = feedparser.parse(url_24h)
            sta = len(feed_24h.entries) # Short-Term Average (24h)
            
            # Fetch 7d (Long-Term Baseline)
            url_7d = f"https://news.google.com/rss/search?q={urllib.parse.quote(keywords)}+when:7d&hl=en-US&gl=US&ceid=US:en"
            feed_7d = feedparser.parse(url_7d)
            lta = len(feed_7d.entries) / 7.0 # Long-Term Average (Daily)
            
            golden_cross = False
            # Trigger Golden Cross if STA is 50% higher than LTA, and total hits < 50 (not priced in)
            if sta > (lta * 1.5) and lta > 0 and sta < 50:
                golden_cross = True
                
            headlines = [{"title": entry.title, "link": entry.link} for entry in feed_24h.entries[:5]]
                
            return {
                "sta_hits": sta,
                "lta_hits": round(lta, 1),
                "golden_cross": golden_cross,
                "details": {
                    "headlines": headlines,
                    "description": "Momentum tracks positive institutional/crypto news over 24h vs 7d average. A 'Golden Cross' triggers automated BUY signals."
                }
            }
        except Exception as e:
            logger.warning(f"Error fetching positive momentum: {e}")
            return {"sta_hits": 0, "lta_hits": 0, "golden_cross": False, "details": {}}
            
    def get_gold_correlation(self):
        return self._get_cached("gold", 3600, self._fetch_gold_correlation)
        
    def _fetch_gold_correlation(self):
        """Calculate 30-day Pearson correlation between BTC and Gold (GC=F)"""
        try:
            # Fetch 30 days of data
            tickers = yf.Tickers("BTC-USD GC=F")
            hist = tickers.history(period="30d")
            
            if hist.empty or "Close" not in hist.columns:
                return {"correlation": 0.0, "details": {}}
                
            # Get closing prices, drop NaNs
            closes = hist["Close"].dropna()
            
            if "BTC-USD" not in closes.columns or "GC=F" not in closes.columns:
                return {"correlation": 0.0, "details": {}}
                
            btc_prices = closes["BTC-USD"]
            gold_prices = closes["GC=F"]
            
            correlation = btc_prices.corr(gold_prices)
            
            return {
                "correlation": round(correlation, 3) if not pd.isna(correlation) else 0.0,
                "details": {
                    "description": "30-Day Pearson correlation coefficient between BTC-USD and Gold Futures (GC=F). Values > 0.7 indicate BTC acting strongly as a safe-haven asset."
                }
            }
        except Exception as e:
            logger.warning(f"Error fetching gold correlation: {e}")
            return {"correlation": 0.0, "details": {}}
            
    def get_social_volume(self):
        return self._get_cached("social", 3600, self._fetch_social_volume)
        
    def _fetch_social_volume(self):
        """Fetch Google Trends relative interest for high-risk keywords"""
        try:
            from pytrends.request import TrendReq
            pt = TrendReq(hl='en-US', tz=360, timeout=(10,25))
            pt.build_payload(["war", "recession"], cat=0, timeframe='now 1-d', geo='', gprop='')
            df = pt.interest_over_time()
            if df.empty:
                return {"war_trend": 0, "recession_trend": 0}
            war_val = int(df["war"].iloc[-1]) if "war" in df.columns else 0
            rec_val = int(df["recession"].iloc[-1]) if "recession" in df.columns else 0
            return {"war_trend": war_val, "recession_trend": rec_val}
        except Exception as e:
            logger.warning(f"Error fetching social volume (pytrends): {e}")
            return {"war_trend": 0, "recession_trend": 0}
