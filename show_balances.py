import pandas as pd
from db_manager import get_all_transactions

def show_portfolio():
    print("🧮 Berechne aktuelle Bestände aus der Datenbank...")
    df = get_all_transactions()
    
    if df.empty:
        print("⚠️ Datenbank ist leer!")
        return

    balances = {}

    for index, row in df.iterrows():
        asset = str(row['asset']).upper()
        amount = float(row['amount'])
        price_eur = float(row['price_eur']) if pd.notna(row['price_eur']) else 0.0
        tx_type = str(row['tx_type']).lower()
        
        fee_amount = float(row['fee_amount']) if pd.notna(row['fee_amount']) else 0.0
        fee_asset = str(row['fee_asset']).upper() if pd.notna(row['fee_asset']) else ""

        # Konten initialisieren
        if asset not in balances: balances[asset] = 0.0
        if 'EUR' not in balances: balances['EUR'] = 0.0
        if fee_amount > 0 and fee_asset not in balances: balances[fee_asset] = 0.0

        # --- DIE NEUE BUCHHALTUNGS-LOGIK ---
        if tx_type == 'buy':
            balances[asset] += amount
            balances['EUR'] -= (amount * price_eur)
            
        elif tx_type == 'sell':
            balances[asset] -= amount
            balances['EUR'] += (amount * price_eur)
            
        elif tx_type in ['deposit', 'staking', 'bonus']: # <-- NEU: Staking und Bonus addieren!
            balances[asset] += amount
            
        elif tx_type == 'withdrawal':
            balances[asset] -= amount

        # Gebühren abziehen
        if fee_amount > 0 and fee_asset:
            balances[fee_asset] -= fee_amount

    print("\n💰 DEIN KRYPTO-PORTFOLIO:")
    print("-" * 40)
    for coin, amt in balances.items():
        if abs(amt) > 0.000001:  # Ignoriert winzige Staub-Reste
            print(f"👉 {coin:<5} | Bestand: {amt:.6f}")
    print("-" * 40)

if __name__ == "__main__":
    show_portfolio()