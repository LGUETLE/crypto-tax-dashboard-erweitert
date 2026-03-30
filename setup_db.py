import sqlite3
import os

def create_database():
    db_name = "crypto_portfolio.db"
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    print(f"🔨 Erstelle Datenbank-Schema in '{db_name}'...")

    # Tabelle für ALLE Transaktionen (Der Ledger)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        exchange TEXT NOT NULL,
        tx_type TEXT NOT NULL,
        asset TEXT NOT NULL,
        amount REAL NOT NULL,
        price_eur REAL,
        fee_amount REAL,
        fee_asset TEXT,
        is_bot BOOLEAN DEFAULT 0,
        tx_hash TEXT UNIQUE, 
        notes TEXT
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset ON transactions(asset)')

    conn.commit()
    conn.close()
    
    print("✅ Datenbank erfolgreich erstellt! Das Fundament steht.")

if __name__ == "__main__":
    create_database()