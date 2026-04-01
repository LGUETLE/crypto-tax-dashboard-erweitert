import os
import glob
import pandas as pd
from db_manager import insert_transaction

CSV_FOLDER = "csv_imports"

def process_bison_row(row, index):
    try:
        tx_hash = str(row['Transaction ID']).strip()
        raw_type = str(row['Transaction type']).strip().lower()
        timestamp = str(row['Date (UTC - Coordinated Universal Time)']).strip()
        fee = float(row['Fee']) if pd.notna(row['Fee']) else 0.0
        
        asset_str = str(row['Asset']).strip().upper() if pd.notna(row['Asset']) else ""
        currency_str = str(row['Currency']).strip().upper() if pd.notna(row['Currency']) else ""
        
        asset, amount, price_eur, tx_type = "", 0.0, 0.0, raw_type

        if raw_type in ['buy', 'sell']:
            asset = asset_str
            amount = float(row['Asset (amount)'])
            price_eur = float(row['Asset (market price)'])
        elif raw_type in ['deposit', 'withdraw', 'withdrawal', 'payout']:
            if asset_str and asset_str != 'NAN':
                asset = asset_str
                amount = float(row['Asset (amount)'])
                tx_type = 'withdrawal' if raw_type in ['withdraw', 'withdrawal', 'payout'] else 'deposit'
            else:
                asset = currency_str
                amount = float(row['Eur (amount)'])
                price_eur = 1.0
                tx_type = 'withdrawal' if raw_type in ['withdraw', 'withdrawal', 'payout'] else 'deposit'

        if asset and tx_type:
            insert_transaction(timestamp, "Bison", tx_type, asset, amount, price_eur, fee, "EUR", False, tx_hash, "Bison CSV")
            return True
    except Exception as e:
        print(f"   ❌ Fehler in Bison Zeile {index+2}: {e}")
        return False
    return False

def process_bitvavo_row(row, index):
    try:
        status = str(row.get('Status', '')).strip().lower()
        if status not in ['completed', 'distributed']: return False

        timestamp = f"{str(row['Date']).strip()} {str(row['Time']).strip()}"
        raw_type = str(row['Type']).strip().lower()
        asset = str(row['Currency']).strip().upper()
        amount = float(row['Amount']) if pd.notna(row['Amount']) else 0.0
        
        price_eur = float(row['Quote Price']) if pd.notna(row['Quote Price']) else (1.0 if asset == 'EUR' else 0.0)
        fee_amount = float(row['Fee amount']) if pd.notna(row['Fee amount']) else 0.0
        fee_asset = str(row['Fee currency']).strip().upper() if pd.notna(row['Fee currency']) else ""

        tx_hash = str(row['Transaction ID']).strip()
        if pd.isna(row['Transaction ID']) or not tx_hash or tx_hash == 'nan':
            tx_hash = f"bitvavo_{timestamp}_{raw_type}_{amount}_{asset}".replace(" ", "_")

        tx_type = 'staking' if raw_type in ['staking', 'fixed_staking'] else 'bonus' if raw_type in ['rebate', 'campaign_new_user_incentive'] else raw_type
        
        insert_transaction(timestamp, "Bitvavo", tx_type, asset, amount, price_eur, fee_amount, fee_asset, False, tx_hash, "Bitvavo CSV")
        return True
    except Exception as e:
        print(f"   ❌ Fehler in Bitvavo Zeile {index+2}: {e}")
        return False
    return False

def run_master_importer():
    if not os.path.exists(CSV_FOLDER):
        print(f"📁 Ordner '{CSV_FOLDER}' fehlt.")
        return

    for file in glob.glob(os.path.join(CSV_FOLDER, "*.csv")):
        filename = os.path.basename(file).lower()
        print(f"\n📥 Prüfe Datei: {filename}")
        
        if "bison" in filename:
            print("   -> 🦬 Erkenne Bison-Format...")
            df = pd.read_csv(file, sep=';')
            df.columns = df.columns.str.strip() # DAS HAT GEFEHLT!
            success = sum(1 for i, row in df.iterrows() if process_bison_row(row, i))
            print(f"   ✅ {success} Transaktionen importiert/geprüft.")
            
        elif "bitvavo" in filename:
            print("   -> 🇳🇱 Erkenne Bitvavo-Format...")
            df = pd.read_csv(file, sep=',')
            df.columns = df.columns.str.strip() # DAS HAT GEFEHLT!
            success = sum(1 for i, row in df.iterrows() if process_bitvavo_row(row, i))
            print(f"   ✅ {success} Transaktionen importiert/geprüft.")
            
        else:
            print(f"   ⚠️ Überspringe Datei. Weder 'bison' noch 'bitvavo' im Dateinamen.")

    print("\n🏁 Master-Import abgeschlossen!")

if __name__ == "__main__":
    run_master_importer()