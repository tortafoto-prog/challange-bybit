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
        
        self.domain_suffix = "" # default (global .com)
        if self.is_eu:
            self.domain_suffix = "bytick"  # pybit uses 'bytick' for EU endpoints
            
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
            # Category 'linear' for USDT Perps
            # IMPORTANT: Specify category in the request to ensure we get category field in response
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
                try:
                    self.process_trade_data(exec_item, is_history=False)
                except Exception as item_error:
                    print(f"[{self.user_name}] Error processing execution item: {item_error}")
                    # Continue processing other items even if one fails
        except Exception as e:
            print(f"[{self.user_name}] Error processing message: {e}")


    def process_trade_data(self, data, is_history=False):
        # Bybit V5 Execution Data Fields:
        # category: spot, linear, inverse, option
        # symbol, side (Buy/Sell), execPrice, execQty, execId, orderId
        # closedSize (if position was closed)
        
        # FILTER: Only process Linear (USDT Perpetual/Futures) trades
        # Ignore: spot, inverse, option
        # NOTE: History sync already filters by category='linear', so this mainly affects live stream
        category = data.get('category', 'linear')  # Default to linear if missing (for history items)
        if category != 'linear':
            print(f"[{self.user_name}] Ignoring {category} trade: {data.get('symbol')}")
            return
        
        symbol = data['symbol']

        side = data['side'].upper() # BUY / SELL
        price = float(data['execPrice'])
        volume = float(data['execQty'])
        ticket_id = str(data['execId']) # Individual fill ID
        order_id = str(data['orderId'])
        
        # Timestamp
        ts = int(data['execTime'])
        open_time_str = self.convert_timestamp(ts)
        
        # Detect if this is a CLOSING trade
        # Bybit provides 'closedSize' field - if > 0, it means position was reduced/closed
        # IMPORTANT: closedSize can be empty string '', must handle safely
        closed_size_raw = data.get('closedSize', '0')
        try:
            closed_size = float(closed_size_raw) if closed_size_raw else 0.0
        except (ValueError, TypeError):
            closed_size = 0.0
        
        is_closing = "Yes" if closed_size > 0 else "No"
        
        # SL/TP Extraction
        # Bybit Execution stream does NOT include SL/TP!
        # We must query Position Info API to get these values
        stop_loss = ""
        take_profit = ""
        
        if is_closing == "No":
            # Only fetch SL/TP for opening trades
            # Try Position API first (for existing positions)
            sl_tp = self.get_position_sl_tp(symbol)
            
            # If no position found (e.g., position flip Long->Short), check open orders
            if not sl_tp or (not sl_tp.get('stopLoss') and not sl_tp.get('takeProfit')):
                sl_tp = self.get_open_orders_sl_tp(symbol)
            
            if sl_tp:
                stop_loss = sl_tp.get('stopLoss', '')
                take_profit = sl_tp.get('takeProfit', '')
        
        # Position ID logic: Ticker_SL_Volume
        position_id = f"{symbol}_{stop_loss}_{volume}"
        
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
            "is_closing": is_closing,
            "order_type": data.get('orderType', 'MARKET'),
            "position_id": position_id,
            "open_time": open_time_str,
            "comment": "Bybit History" if is_history else "Bybit Stream"
        }
        
        print(f"[{self.user_name}] TRADE: {symbol} {side} {volume} @ {price} (SL: {stop_loss}, TP: {take_profit})")
        self.send_webhook(trade_data)

    def get_position_sl_tp(self, symbol):
        """Query Position Info API to get SL/TP for a symbol."""
        try:
            resp = self.session.get_positions(
                category="linear",
                symbol=symbol
            )
            
            if resp['retCode'] == 0:
                positions = resp['result'].get('list', [])
                if positions:
                    # Return first position (usually only one per symbol in one-way mode)
                    pos = positions[0]
                    return {
                        'stopLoss': pos.get('stopLoss', ''),
                        'takeProfit': pos.get('takeProfit', '')
                    }
            else:
                print(f"[{self.user_name}] Position API Error: {resp}")
        except Exception as e:
            print(f"[{self.user_name}] Failed to fetch Position SL/TP: {e}")
        
        return None

    def get_open_orders_sl_tp(self, symbol):
        """Query Open Orders API to get SL/TP from conditional orders.
        This is a fallback for when position flips (Long->Short) and SL/TP is in pending orders.
        """
        try:
            resp = self.session.get_open_orders(
                category="linear",
                symbol=symbol
            )
            
            if resp['retCode'] == 0:
                orders = resp['result'].get('list', [])
                
                sl = ''
                tp = ''
                
                # Look for Stop Loss and Take Profit orders
                for order in orders:
                    order_type = order.get('orderType', '')
                    stop_order_type = order.get('stopOrderType', '')
                    trigger_price = order.get('triggerPrice', '')
                    
                    # Stop Loss: stopOrderType contains 'Stop'
                    if 'Stop' in stop_order_type and trigger_price:
                        sl = trigger_price
                    
                    # Take Profit: stopOrderType contains 'TakeProfit' or order is limit with trigger
                    if 'TakeProfit' in stop_order_type and trigger_price:
                        tp = trigger_price
                
                if sl or tp:
                    return {'stopLoss': sl, 'takeProfit': tp}
            else:
                print(f"[{self.user_name}] Open Orders API Error: {resp}")
        except Exception as e:
            print(f"[{self.user_name}] Failed to fetch Open Orders SL/TP: {e}")
        
        return None


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
