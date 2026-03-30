import sqlite3
import pandas as pd

DB_NAME = "crypto_portfolio.db"

def insert_transaction(timestamp, exchange, tx_type, asset, amount, price_eur, fee_amount, fee_asset, is_bot, tx_hash, notes=""):
    """Schreibt eine einzelne Transaktion sicher in die Datenbank."""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO transactions (timestamp, exchange, tx_type, asset, amount, price_eur, fee_amount, fee_asset, is_bot, tx_hash, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, exchange, tx_type, asset, amount, price_eur, fee_amount, fee_asset, is_bot, tx_hash, notes))
        
        conn.commit()
        # print(f"✅ Transaktion gespeichert: {tx_type} {amount} {asset} auf {exchange}")
        
    except sqlite3.IntegrityError:
        print(f"⚠️ Übersprungen: Transaktion {tx_hash} existiert bereits (Duplikat).")
    except Exception as e:
        print(f"❌ Fehler beim Speichern: {e}")
    finally:
        conn.close()

def get_all_transactions():
    """Lädt alle Transaktionen als praktischen Pandas DataFrame für das Dashboard."""
    conn = sqlite3.connect(DB_NAME)
    query = "SELECT * FROM transactions ORDER BY timestamp DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- TEST-BEREICH ---
if __name__ == "__main__":
    print("🧪 Starte Testlauf für die Datenbank...")
    
    # 1. Einen fiktiven Bison-Kauf aus der Vergangenheit eintragen
    insert_transaction(
        timestamp="2023-11-15 14:30:00",
        exchange="Bison",
        tx_type="buy",
        asset="BTC",
        amount=0.15,
        price_eur=34500.00,  # BTC Preis damals in Euro
        fee_amount=0.00,     # Bison hat den Spread, oft 0 direkte Gebühr
        fee_asset="EUR",
        is_bot=False,
        tx_hash="bison_test_001", # Einzigartige ID
        notes="Manueller Test-Import"
    )
    
    # 2. Daten abrufen und anzeigen
    df = get_all_transactions()
    print("\n📊 Aktueller Datenbank-Inhalt:")
    print(df[['timestamp', 'exchange', 'tx_type', 'amount', 'asset', 'price_eur']])