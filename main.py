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
        
        # Determine environment (Live or Testnet priority? Usually we run one)
        # We'll check if 'live' keys exist first, or 'testnet' based on a global flag?
        # Actually in Binance client we had use_testnet passed in. 
        # Typically we use a global USE_TESTNET env var to decide which set of keys to pick from the JSON.
        
        import os
        use_testnet = os.getenv("USE_TESTNET", "True").lower() == "true"
        
        creds = user_conf.get("testnet") if use_testnet else user_conf.get("live")
        
        if not creds:
            print(f"[{name}] Skipping: No credentials for {'TESTNET' if use_testnet else 'LIVE'}")
            continue
            
        api_key = creds.get("key")
        api_secret = creds.get("secret")
        
        if not api_key:
            print(f"[{name}] Skipping: Incomplete credentials.")
            continue
            
        # Initialize Client
        client = BybitClient(
            api_key=api_key,
            api_secret=api_secret,
            use_testnet=use_testnet,
            user_id=uid,
            user_name=name,
            is_eu=is_eu
        )
        
        clients.append(client)
        
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
