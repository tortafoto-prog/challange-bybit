import time
import signal
import sys
from config import Config
from bybit_client import BybitClient

def main():
    print("--- Bybit Multi-User Client V1.0 ---")
    
    clients = []
    
    # Iterate over configured users
    for user_conf in Config.USERS:
        name = user_conf.get("name", "Unknown")
        uid = user_conf.get("id", "Unknown")
        is_eu = user_conf.get("is_eu", False)
        
        # Mixed Mode Logic:
        # A user can have BOTH Live and Testnet credentials active simultaneously.
        # We will check both "live" and "testnet" keys and spawn a client for each if found.
        
        # 1. Check LIVE Credentials
        if user_conf.get("live") and user_conf["live"].get("key"):
            creds = user_conf["live"]
            print(f"[{name}] Found LIVE credentials ({'EU' if is_eu else 'Global'}). Initializing...")
            
            client_live = BybitClient(
                api_key=creds.get("key"),
                api_secret=creds.get("secret"),
                use_testnet=False,
                user_id=uid,
                user_name=name,
                is_eu=is_eu
            )
            clients.append(client_live)

        # 2. Check TESTNET Credentials
        if user_conf.get("testnet") and user_conf["testnet"].get("key"):
            creds = user_conf["testnet"]
            print(f"[{name}] Found TESTNET credentials ({'EU' if is_eu else 'Global'}). Initializing...")
            
            client_test = BybitClient(
                api_key=creds.get("key"),
                api_secret=creds.get("secret"),
                use_testnet=True,
                user_id=uid,
                user_name=name,
                is_eu=is_eu
            )
            clients.append(client_test)
        
        if not clients:
             # This check is slightly loose because clients list grows globally, 
             # but serves to warn if a specific user block resulted in no clients.
             pass

        
    if not clients:
        print("No valid clients configured. Exiting.")
        return

    # Start all clients
    print(f"Starting {len(clients)} clients...")
    for c in clients:
        c.start()
        
    # Keep main thread alive
    running = True
    def signal_handler(sig, frame):
        nonlocal running
        print("\nRequesting shutdown...")
        running = False
        for c in clients:
            c.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    
    while running:
        time.sleep(1)

if __name__ == "__main__":
    main()
