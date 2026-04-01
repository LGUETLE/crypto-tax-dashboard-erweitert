import os
import ccxt
from dotenv import load_dotenv
from db_manager import insert_transaction, get_all_transactions

load_dotenv()

def sync_bitvavo_api():
    print("🔄 Verbinde mit der Bitvavo API (Sniper-Mode)...")
    
    exchange = ccxt.bitvavo({
        'apiKey': os.getenv('BITVAVO_API_KEY'),
        'secret': os.getenv('BITVAVO_SECRET'),
        'enableRateLimit': True 
    })

    try:
        new_records = 0
        
        print("📡 Analysiere relevante Märkte...")
        df_db = get_all_transactions()
        db_coins = set(df_db['asset'].dropna().unique()) if not df_db.empty else set()
        
        balance = exchange.fetch_balance()
        live_coins = set(coin for coin, amount in balance['total'].items() if amount > 0)
        
        my_coins = db_coins.union(live_coins)
        eur_markets = [f"{coin}/EUR" for coin in my_coins if coin != 'EUR']
        
        print(f"🎯 Scan reduziert auf {len(eur_markets)} relevante Märkte.")

        exchange.load_markets() 
        
        for symbol in eur_markets:
            if symbol not in exchange.markets: continue 
            try:
                trades = exchange.fetch_my_trades(symbol)
                for trade in trades:
                    tx_hash = str(trade['id'])
                    timestamp = trade['datetime'].replace('T', ' ').split('.')[0] 
                    raw_type = trade['side'].lower()
                    asset = trade['symbol'].split('/')[0]
                    amount = float(trade['amount'])
                    price_eur = float(trade['price'])
                    fee_amount = float(trade['fee'].get('cost', 0.0)) if trade.get('fee') else 0.0
                    fee_asset = str(trade['fee'].get('currency', '')).upper() if trade.get('fee') else ""
                    try:
                        insert_transaction(timestamp, "Bitvavo", raw_type, asset, amount, price_eur, fee_amount, fee_asset, False, f"bitvavo_api_trade_{tx_hash}", "Bitvavo API Trade")
                        new_records += 1
                    except: pass 
            except: pass

        print("🏦 Prüfe Ein- und Auszahlungen...")
        for dep in exchange.fetch_deposits():
            try:
                tx_hash = str(dep['id'])
                timestamp = dep['datetime'].replace('T', ' ').split('.')[0]
                asset = str(dep['currency']).upper()
                amount = float(dep['amount'])
                fee = float(dep['fee']['cost']) if dep.get('fee') else 0.0
                insert_transaction(timestamp, "Bitvavo", 'deposit', asset, amount, 1.0 if asset == 'EUR' else 0.0, fee, asset if fee > 0 else "", False, f"bitvavo_api_dep_{tx_hash}", "Bitvavo API Deposit")
                new_records += 1
            except: pass

        for wdl in exchange.fetch_withdrawals():
            try:
                tx_hash = str(wdl['id'])
                timestamp = wdl['datetime'].replace('T', ' ').split('.')[0]
                asset = str(wdl['currency']).upper()
                amount = float(wdl['amount'])
                fee = float(wdl['fee']['cost']) if wdl.get('fee') else 0.0
                insert_transaction(timestamp, "Bitvavo", 'withdrawal', asset, amount, 1.0 if asset == 'EUR' else 0.0, fee, asset if fee > 0 else "", False, f"bitvavo_api_wdl_{tx_hash}", "Bitvavo API Withdrawal")
                new_records += 1
            except: pass

        print(f"\n✅ API-Sync fertig! {new_records} neue Einträge gespeichert.")
        print("💡 WICHTIG: Staking-Rewards gibt die Bitvavo API leider NICHT aus. Dafür gelegentlich die CSV nutzen.")

    except Exception as e:
        print(f"❌ Schwerer API-Fehler: {e}")

if __name__ == "__main__":
    sync_bitvavo_api()