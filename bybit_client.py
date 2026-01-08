import time
import threading
import json
import requests
from datetime import datetime, timedelta
from pybit.unified_trading import WebSocket, HTTP
from config import Config

class BybitClient:
    def __init__(self, api_key, api_secret, use_testnet=True, user_id="Unknown", user_name="Unknown", is_eu=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.use_testnet = use_testnet
        self.is_eu = is_eu
        self.user_id = user_id
        
        # Determine Environment Label
        self.env_label = "TESTNET" if use_testnet else "LIVE"
        self.region_label = "EU" if is_eu else "GLOBAL"
        self.user_name = f"{user_name}:{self.env_label}:{self.region_label}"
        
        self.running = False
        self.ws = None
        self.session = None # HTTP Session
        self.last_start_time = 0
        self.reconnect_delay = 5
        
        # Setup Endpoints (pybit handles URLs mostly, but we specify testnet boolean and suffix)
        # For .eu, pybit might need 'domain' arg or manual URL override?
        # Checking pybit docs: WebSocket(testnet=True, domain="bybit" or "byTick" or "bybit.eu"?)
        # Pybit unified_trading.WebSocket supports 'channel_type="private"'
        # Arg 'testnet': bool
        # Arg 'domain': str (default empty implies .com). Set to 'bybit.eu' for EU?
        # Let's assume 'bybit' is global, 'bybit.eu' is EU.
        # Actually standard is:
        # Global: stream.bybit.com
        # EU: stream.bybit.eu
        # Pybit typically maps 'domain' to specific presets.
        
        self.domain_suffix = "bybit" # default
        if self.is_eu:
            self.domain_suffix = "bybit.eu" # Verify this mapping in pybit
            
        # Initialize HTTP Session (for History Sync / REST calls)
        try:
            self.session = HTTP(
                testnet=self.use_testnet,
                api_key=self.api_key,
                api_secret=self.api_secret,
                domain=self.domain_suffix
            )
        except Exception as e:
            print(f"[{self.user_name}] Error initializing HTTP Session: {e}")

    def start(self):
        self.running = True
        print(f"[{self.user_name}] Starting Client...")
        
        # 1. Sync History (Catch-up)
        if self.session:
            self.sync_history()
            
        # 2. Start WebSocket
        try:
            # channel_type="private" for executions/orders
            self.ws = WebSocket(
                testnet=self.use_testnet,
                api_key=self.api_key,
                api_secret=self.api_secret,
                channel_type="private",
                domain=self.domain_suffix
            )
            
            # Subscribe to 'execution' topic
            # V5 Topic: 'execution'
            self.ws.execution_stream(callback=self.on_execution)
            
            print(f"[{self.user_name}] WebSocket Connected & Subscribed to Executions.")
            
        except Exception as e:
            print(f"[{self.user_name}] Failed to start WebSocket: {e}")
            
        # 3. Start Watchdog
        self.last_start_time = time.time()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()

    def stop(self):
        print(f"[{self.user_name}] Stopping client...")
        self.running = False
        if self.ws:
            # Pybit ws doesn't have a simple stop()? 
            # usually exit() or similar.
            # Using internal exit method if available or just letting GC handle it?
            # Pybit WS runs in a thread. 
            # Looking at source usually: ws.exit()
            try:
                self.ws.exit()
            except:
                pass

    def restart(self):
        print(f"[{self.user_name}] Restarting in {self.reconnect_delay} seconds...")
        time.sleep(self.reconnect_delay)
        self.stop()
        self.start()

    def _watchdog_loop(self):
        print(f"[{self.user_name}] Watchdog started. Refresh scheduled in 30 minutes.")
        while self.running:
            time.sleep(60)
            if not self.running: break
            
            elapsed = time.time() - self.last_start_time
            if elapsed > 1800: # 30 mins
                print(f"[{self.user_name}] WATCHDOG: Scheduled Restart...")
                try:
                    self.restart()
                except Exception as e:
                     print(f"[{self.user_name}] Watchdog Restart Failed: {e}")
                break

    def sync_history(self):
        """Fetch recent executions to catch up."""
        print(f"[{self.user_name}] Syncing History...")
        try:
            # Get last 50 executions
            # Category 'linear' for USDT Perps (Standard) or 'inverse'?
            # Usually users trade USDT Perps. V5 'category' is required.
            # We'll check 'linear' (USDT-M) and maybe 'spot' if configured? 
            # Assuming 'linear' for Futures.
            
            resp = self.session.get_executions(category="linear", limit=20)
            if resp['retCode'] == 0:
                result = resp['result']
                exec_list = result.get('list', [])
                print(f"[{self.user_name}] Found {len(exec_list)} recent executions.")
                
                # Sort by time ascending to process properly?
                # List usually returned desc.
                for exc in reversed(exec_list):
                   self.process_trade_data(exc, is_history=True)
            else:
                 print(f"[{self.user_name}] History Sync API Error: {resp}")

        except Exception as e:
            print(f"[{self.user_name}] History Sync Failed: {e}")

    def on_execution(self, message):
        """WebSocket Callback"""
        # message format: {'topic': 'execution', 'id': '...', 'creationTime': ..., 'data': [...]}
        try:
            data_list = message.get('data', [])
            for exec_item in data_list:
                self.process_trade_data(exec_item, is_history=False)
        except Exception as e:
            print(f"[{self.user_name}] Error processing message: {e}")

    def process_trade_data(self, data, is_history=False):
        # Bybit V5 Execution Data Fields:
        # symbol, side (Buy/Sell), execPrice, execQty, execId, orderId
        # isMaker (bool)
        # createTime (ms)
        
        symbol = data['symbol']
        side = data['side'].upper() # BUY / SELL
        price = float(data['execPrice'])
        volume = float(data['execQty'])
        ticket_id = str(data['execId']) # Individual fill ID
        order_id = str(data['orderId'])
        
        # Timestamp
        ts = int(data['execTime'])
        open_time_str = self.convert_timestamp(ts)
        
        # SL/TP? Bybit Execution stream doesn't always send SL/TP.
        # It sends 'stopLoss', 'takeProfit' in the execution item IF created?
        # Actually V5 'execution' topic data has 'stopLog', 'takeProfit' fields?
        # NO, usually it's in the Order stream.
        # But let's check what we have.
        # For now, placeholder or check logic?
        # We'll use empty SL/TP or implement lookups later if needed.
        # Bybit V5 Execution payload usually does NOT have SL/TP values.
        # We might need to query the Order if we really need them.
        # But for 'Tracker', usually the User ensures SL is set.
        
        stop_loss = "" # Todo: Fetch if critical
        take_profit = ""
        
        # Position ID logic
        # Opening: symbol_sl_vol ?
        # Closing: ?
        # Bybit has 'closedSize'? 'execType'? 
        # Logic: If 'closedSize' > 0 ? No, 'execType' == 'Trade'?
        # Bybit treats closing as separate execution.
        # How to detect CLOSING?
        # V5: execType might help? No.
        # Usually we rely on ReduceOnly flag?
        # Data has 'isReduceOnly'? Yes! (In Order stream, maybe not Execution?)
        # Let's check docs or logs. Assuming we treat all as valid fills.
        # For closing logic in Sheet, we just need direction.
        
        # Heuristic: User asked for Position ID = Ticker_SL_Vol
        # Without SL, we can't generate the Opening ID consistently if we rely on it.
        # We will use OrderID as fallback PositionID for now? 
        # Or Ticker_0_Volume.
        
        position_id = f"{symbol}_{stop_loss}_{volume}"
        
        # Is Closing?
        # If we can't tell, send "Unknown". Sheet logic handles FIFO.
        # But if we want proper "Matched" comment...
        # Let's default to "No" unless we know.
        
        trade_data = {
            "secret_key": Config.APP_SECRET,
            "account_id": self.user_id,
            "user_id": self.user_id,
            "symbol": symbol,
            "direction": side,
            "price": price,
            "volume": volume,
            "ticket_id": ticket_id,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "is_closing": "Unknown", # Sheet will solve
            "order_type": "MARKET",
            "position_id": position_id,
            "open_time": open_time_str,
            "comment": "Bybit History" if is_history else "Bybit Stream"
        }
        
        print(f"[{self.user_name}] TRADE: {symbol} {side} {volume} @ {price}")
        self.send_webhook(trade_data)

    def convert_timestamp(self, ts_ms):
        try:
            utc_time = datetime.utcfromtimestamp(ts_ms / 1000.0)
            hungarian_time = utc_time + timedelta(hours=1) 
            return hungarian_time.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return ""

    def send_webhook(self, data):
        threading.Thread(target=self._send_webhook_process, args=(data,), daemon=True).start()

    def _send_webhook_process(self, data):
        if Config.GOOGLE_WEBHOOK_URL:
            try:
                requests.post(Config.GOOGLE_WEBHOOK_URL, json=data, timeout=30)
            except Exception as e:
                print(f"[{self.user_name}] Webhook Failed: {e}")
