import time
import threading
import json
import requests
import os
from datetime import datetime, timedelta
from datetime import datetime, timedelta
from pybit.unified_trading import WebSocket, HTTP
from config import Config

class BybitClient:
    CACHE_FILE = "processed_tickets.json"

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
        
        # Execution Aggregation (for partial fills)
        # {orderId: {'data': first_exec_data, 'total_volume': float, 'total_closed': float, 'timer': threading.Timer}}
        self.pending_executions = {}
        self.aggregation_lock = threading.Lock()
        self.aggregation_delay = 1.0  # Wait 1 second to collect all fills from same order
        
        # CACHE: Local storage for processed Ticket IDs to speed up history sync
        self.processed_tickets = set()
        self.load_cache()
        
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

    def load_cache(self):
        """Load processed Ticket IDs from local JSON file."""
        if os.path.exists(self.CACHE_FILE):
            try:
                with open(self.CACHE_FILE, 'r') as f:
                    data = json.load(f)
                    self.processed_tickets = set(data.get('tickets', []))
                print(f"[{self.user_name}] Loaded {len(self.processed_tickets)} tickets from cache.")
            except Exception as e:
                print(f"[{self.user_name}] Warning: Failed to load cache: {e}")
        else:
             print(f"[{self.user_name}] No cache file found. Starting fresh.")

    def save_cache(self):
        """Save processed Ticket IDs to local JSON file."""
        try:
            with open(self.CACHE_FILE, 'w') as f:
                json.dump({'tickets': list(self.processed_tickets)}, f)
        except Exception as e:
            print(f"[{self.user_name}] Warning: Failed to save cache: {e}")

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
            resp = self.session.get_executions(category="linear", limit=50)
            if resp['retCode'] == 0:
                result = resp['result']
                exec_list = result.get('list', [])
                print(f"[{self.user_name}] Found {len(exec_list)} recent executions.")
                
                # Sort by time ascending to process properly?
                # List usually returned desc.
                
                # AGGREGATION for History (Merge partial fills by OrderID)
                aggregated_history = {}
                
                for exc in reversed(exec_list): # Process oldest to newest
                     order_id = exc.get('orderId')
                     if not order_id:
                         # Fallback if no orderId (should not happen)
                         self.process_trade_data(exc, is_history=True)
                         continue
                     
                     if order_id not in aggregated_history:
                         # New Order ID found
                         aggregated_history[order_id] = exc.copy()
                         aggregated_history[order_id]['execQty'] = float(exc['execQty'])
                         
                         closed_size = exc.get('closedSize', '0')
                         try: closed_size = float(closed_size) if closed_size else 0.0
                         except: closed_size = 0.0
                         aggregated_history[order_id]['closedSize'] = closed_size # Store generic float
                         
                     else:
                         # Already exists, accumulating volume
                         aggregated_history[order_id]['execQty'] += float(exc['execQty'])
                         
                         closed_size = exc.get('closedSize', '0')
                         try: closed_size = float(closed_size) if closed_size else 0.0
                         except: closed_size = 0.0
                         aggregated_history[order_id]['closedSize'] += closed_size

                # Process the Aggregated History Items
                for order_id, agg_data in aggregated_history.items():
                    # Convert back to string format for consistency
                    agg_data['execQty'] = str(agg_data['execQty'])
                    agg_data['closedSize'] = str(agg_data['closedSize'])
                    
                    print(f"[{self.user_name}] History Processing OrderID {order_id} (Total Vol: {agg_data['execQty']})")
                    self.process_trade_data(agg_data, is_history=True, blocking=True)
                    
                    # Rate Limit for History: Sleep to prevent overwhelming Google Sheet
                    # GAS sync can fail if hit by too many simultaneous requests
                    time.sleep(0.5)
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


    def process_trade_data(self, data, is_history=False, blocking=False):
        # Bybit V5 Execution Data Fields:
        # category: spot, linear, inverse, option
        # symbol, side (Buy/Sell), execPrice, execQty, execId, orderId
        # closedSize (if position was closed)
        
        # FILTER: Only process Linear (USDT Perpetual/Futures) trades
        # Ignore: spot, inverse, option
        category = data.get('category', 'linear')  # Default to linear if missing (for history items)
        if category != 'linear':
            print(f"[{self.user_name}] Ignoring {category} trade: {data.get('symbol')}")
            return
            
        # FILTER: Ignore Zero Volume (Invalid)
        raw_qty = float(data.get('execQty', 0))
        if raw_qty <= 0:
            return
            
        # FILTER: Ignore UNKNOWN order types (often cancelled/unfilled debris)
        if data.get('orderType') == 'UNKNOWN':
             print(f"[{self.user_name}] Ignoring UNKNOWN order type: {data.get('symbol')}")
             return
        
        # For history sync, process immediately without aggregation
        if is_history:
            self._process_single_execution(data, is_history=True, blocking=blocking)
            return
        
        # For live stream, aggregate by OrderID
        order_id = str(data.get('orderId', ''))
        if not order_id:
            print(f"[{self.user_name}] Warning: Execution without OrderID, processing immediately")
            self._process_single_execution(data, is_history=False, blocking=blocking)
            return
        
        with self.aggregation_lock:
            if order_id in self.pending_executions:
                # Add to existing aggregation
                pending = self.pending_executions[order_id]
                pending['total_volume'] += float(data.get('execQty', 0))
                
                closed_size_raw = data.get('closedSize', '0')
                try:
                    closed_size = float(closed_size_raw) if closed_size_raw else 0.0
                except (ValueError, TypeError):
                    closed_size = 0.0
                pending['total_closed'] += closed_size
                
                # Cancel old timer and restart
                if pending['timer']:
                    pending['timer'].cancel()
                
                pending['timer'] = threading.Timer(
                    self.aggregation_delay,
                    self._flush_aggregated_execution,
                    args=(order_id,)
                )
                pending['timer'].start()
                
                print(f"[{self.user_name}] Aggregating OrderID {order_id}: {pending['total_volume']} total volume")
            else:
                # First execution for this OrderID
                closed_size_raw = data.get('closedSize', '0')
                try:
                    closed_size = float(closed_size_raw) if closed_size_raw else 0.0
                except (ValueError, TypeError):
                    closed_size = 0.0
                
                timer = threading.Timer(
                    self.aggregation_delay,
                    self._flush_aggregated_execution,
                    args=(order_id,)
                )
                timer.start()
                
                self.pending_executions[order_id] = {
                    'data': data.copy(),
                    'total_volume': float(data.get('execQty', 0)),
                    'total_closed': closed_size,
                    'timer': timer
                }
                
                print(f"[{self.user_name}] Started aggregation for OrderID {order_id}")
    
    def _flush_aggregated_execution(self, order_id):
        """Send aggregated execution after delay."""
        with self.aggregation_lock:
            if order_id not in self.pending_executions:
                return
            
            pending = self.pending_executions.pop(order_id)
            data = pending['data']
            total_volume = pending['total_volume']
            total_closed = pending['total_closed']
        
        # Update data with aggregated values
        data['execQty'] = str(total_volume)
        data['closedSize'] = str(total_closed)
        
        print(f"[{self.user_name}] Flushing aggregated OrderID {order_id}: {total_volume} volume, {total_closed} closed")
        self._process_single_execution(data, is_history=False)
    
    def _process_single_execution(self, data, is_history=False, blocking=False):
        """Process a single (possibly aggregated) execution."""
        symbol = data['symbol']

        side = data['side'].upper() # BUY / SELL
        price = float(data['execPrice'])
        volume = float(data['execQty'])
        
        order_id = str(data['orderId'])
        
        order_id = str(data['orderId'])
        
        # UNIFIED ID FIX: Always use OrderID as TicketID
        # This prevents duplicates when a Live trade (ExecID) is later synced from History (OrderID)
        ticket_id = order_id
        
        # Timestamp
        
        # Timestamp
        
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
        
        # CRITICAL LOGIC for Position Flip (Long->Short or Short->Long):
        # If execQty > closedSize, it means:
        #   - Part of the execution closed the old position (closedSize)
        #   - The rest opened a new position in opposite direction (execQty - closedSize)
        # We need to send TWO separate webhooks in this case!
        
        if closed_size > 0 and volume > closed_size:
            # Position FLIP detected!
            print(f"[{self.user_name}] Position FLIP detected: {symbol} {side} {volume} (closed: {closed_size}, opened: {volume - closed_size})")
            
            # 1. Send CLOSING trade
            self._send_trade_webhook(
                data=data,
                symbol=symbol,
                side=side,
                price=price,
                volume=closed_size,  # Only the closed portion
                ticket_id=ticket_id + "_close",
                is_closing="Yes",
                is_history=is_history,
                open_time_str=open_time_str,
                stop_loss="",
                take_profit="",
                blocking=blocking
            )
            
            # 2. Send OPENING trade (for the new position)
            opening_volume = volume - closed_size
            self._send_trade_webhook(
                data=data,
                symbol=symbol,
                side=side,
                price=price,
                volume=opening_volume,
                ticket_id=ticket_id + "_open",
                is_closing="No",
                is_history=is_history,
                open_time_str=open_time_str,
                stop_loss=None,  # Will fetch from API
                take_profit=None,
                blocking=blocking
            )
            return  # Done, both webhooks sent
        
        # Normal case: either pure closing or pure opening
        is_closing = "Yes" if closed_size > 0 else "No"
        
        self._send_trade_webhook(
            data=data,
            symbol=symbol,
            side=side,
            price=price,
            volume=volume,
            ticket_id=ticket_id,
            is_closing=is_closing,
            is_history=is_history,
            open_time_str=open_time_str,
            stop_loss=None,
            take_profit=None,
            blocking=blocking
        )
    
    def _send_trade_webhook(self, data, symbol, side, price, volume, ticket_id, is_closing, is_history, open_time_str, stop_loss=None, take_profit=None, blocking=False):
        """Helper method to send a single trade webhook."""
        # SL/TP Extraction (only for opening trades)
        if stop_loss is None and take_profit is None:  # Only fetch if not provided
            stop_loss = ""
            take_profit = ""
            
            if is_closing == "No":
                # Only fetch SL/TP for opening trades
                print(f"[{self.user_name}] DEBUG: Fetching SL/TP for {symbol}...")
                
                # Try Position API first (for existing positions)
                sl_tp = self.get_position_sl_tp(symbol)
                
                # If no position found (e.g., position flip Long->Short), check open orders
                if not sl_tp or (not sl_tp.get('stopLoss') and not sl_tp.get('takeProfit')):
                    print(f"[{self.user_name}] DEBUG: No Position SL/TP, checking Open Orders...")
                    sl_tp = self.get_open_orders_sl_tp(symbol)
                
                # If still no SL, check Order History (Initial SL on the Order)
                if not sl_tp or (not sl_tp.get('stopLoss') and not sl_tp.get('takeProfit')):
                    print(f"[{self.user_name}] DEBUG: No Open Order SL/TP, checking Order History for OrderID {ticket_id}...")
                    sl_tp = self.get_order_history_sl_tp(symbol, ticket_id)
                
                if sl_tp:
                    stop_loss = sl_tp.get('stopLoss', '')
                    take_profit = sl_tp.get('takeProfit', '')
                    print(f"[{self.user_name}] DEBUG: Found SL: {stop_loss}, TP: {take_profit}")
        else:
            # Use provided values (for position flip closing part)
            if stop_loss is None:
                stop_loss = ""
            if take_profit is None:
                take_profit = ""
        
        # Position ID logic: Symbol_SL_Volume (same as Binance)
        # Google Sheet will handle partial fills via FIFO volume matching
        position_id = f"{symbol}_{stop_loss}_{volume}"
        
        trade_data = {
            "secret_key": Config.APP_SECRET,
            "source": "bybit",
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
        self.send_webhook(trade_data, blocking=blocking)

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
                    # print(f"[{self.user_name}] DEBUG POS: {pos}")
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
                print(f"[{self.user_name}] DEBUG ORDERS: Found {len(orders)} open orders.")
                
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

    def get_order_history_sl_tp(self, symbol, order_id):
        """Query Order History to get Initial SL/TP from the completed order."""
        try:
            resp = self.session.get_order_history(
                category="linear",
                symbol=symbol,
                orderId=order_id,
                limit=1
            )
            
            if resp['retCode'] == 0:
                orders = resp['result'].get('list', [])
                if orders:
                    order = orders[0]
                    # print(f"[{self.user_name}] DEBUG HISTORY ORDER: {order}")
                    return {
                        'stopLoss': order.get('stopLoss', ''),
                        'takeProfit': order.get('takeProfit', '')
                    }
            else:
                 print(f"[{self.user_name}] Order History API Error: {resp}")
        except Exception as e:
             print(f"[{self.user_name}] Failed to fetch Order History SL/TP: {e}")
        
        return None


    def convert_timestamp(self, ts_ms):
        try:
            utc_time = datetime.utcfromtimestamp(ts_ms / 1000.0)
            hungarian_time = utc_time + timedelta(hours=1) 
            return hungarian_time.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return ""

    def send_webhook(self, data, blocking=False):
        # CACHE CHECK: If ticket_id already processed, SKIP
        ticket_id = data.get('ticket_id')
        if ticket_id and ticket_id in self.processed_tickets:
            print(f"[{self.user_name}] [SKIP] Trade already synced: {ticket_id}")
            return

        if blocking:
            self._send_webhook_process(data)
        else:
            threading.Thread(target=self._send_webhook_process, args=(data,), daemon=True).start()

    def _send_webhook_process(self, data):
        if Config.GOOGLE_WEBHOOK_URL:
            try:
                resp = requests.post(Config.GOOGLE_WEBHOOK_URL, json=data, timeout=30)
                
                # If successful (200 OK), save to cache
                if resp.status_code == 200:
                    ticket_id = data.get('ticket_id')
                    if ticket_id:
                        self.processed_tickets.add(ticket_id)
                        self.save_cache()
                        # print(f"[{self.user_name}] Cached ticket: {ticket_id}")
                else:
                    print(f"[{self.user_name}] Webhook Error {resp.status_code}: {resp.text}")
                    
            except Exception as e:
                print(f"[{self.user_name}] Webhook Failed: {e}")
