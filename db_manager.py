import sqlite3
import pandas as pd
from contextlib import contextmanager

DB_NAME = "crypto_portfolio.db"

@contextmanager
def get_db_connection():
    """Stellt eine sichere Verbindung zur Datenbank her (mit Context Manager)."""
    conn = sqlite3.connect(DB_NAME)
    try:
        yield conn
    finally:
        conn.close()

def setup_database():
    """Erstellt die Tabellen für Spot UND Futures, falls sie noch nicht existieren."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # --- TABELLE 1: SPOT TRADES & TRANSFERS (Das kennst du schon) ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                exchange TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount REAL NOT NULL,
                price_eur REAL NOT NULL,
                fee_amount REAL DEFAULT 0,
                fee_asset TEXT,
                is_bot BOOLEAN DEFAULT 0,
                tx_hash TEXT UNIQUE,
                notes TEXT
            )
        ''')

        # --- TABELLE 2: FUTURES TRADES (Neu für deinen Bot!) ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS futures_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,          -- z.B. BTC-USDT-PERP
                position_side TEXT NOT NULL,   -- LONG oder SHORT
                leverage REAL NOT NULL,        -- z.B. 10.0 (Hebel)
                margin_type TEXT,              -- CROSS oder ISOLATED
                amount REAL NOT NULL,          -- Positionsgröße
                entry_price REAL,
                close_price REAL,
                realized_pnl REAL NOT NULL,    -- Gewinn/Verlust (meist in USDT)
                funding_fee REAL DEFAULT 0,    -- Gezahlte/Erhaltene Funding Rate
                is_bot BOOLEAN DEFAULT 1,      -- Fast immer 1, da du es über den Bot machst
                tx_hash TEXT UNIQUE,           -- Order-ID der Börse
                notes TEXT
            )
        ''')
        conn.commit()
    print("✅ Datenbank ist bereit! (Spot & Futures Tabellen geladen)")

# --- SPOT FUNKTIONEN ---

def insert_transaction(timestamp, exchange, tx_type, asset, amount, price_eur, 
                       fee_amount=0.0, fee_asset="", is_bot=False, tx_hash=None, notes=""):
    """Fügt eine SPOT-Transaktion (oder Transfer) hinzu."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO transactions (timestamp, exchange, tx_type, asset, amount, price_eur, fee_amount, fee_asset, is_bot, tx_hash, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, exchange, tx_type, asset, amount, price_eur, fee_amount, fee_asset, is_bot, tx_hash, notes))
        conn.commit()

def get_all_transactions():
    """Holt alle Spot-Transaktionen als Pandas DataFrame."""
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM transactions", conn)
    return df

# --- FUTURES FUNKTIONEN (NEU) ---

def insert_futures_trade(timestamp, exchange, symbol, position_side, leverage, amount, 
                         realized_pnl, funding_fee=0.0, entry_price=0.0, close_price=0.0, 
                         margin_type="CROSS", is_bot=True, tx_hash=None, notes=""):
    """Fügt einen beendeten FUTURES-Trade (PNL) hinzu."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO futures_trades (timestamp, exchange, symbol, position_side, leverage, margin_type, 
                                        amount, entry_price, close_price, realized_pnl, funding_fee, is_bot, tx_hash, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, exchange, symbol, position_side, leverage, margin_type, amount, entry_price, close_price, realized_pnl, funding_fee, is_bot, tx_hash, notes))
        conn.commit()

def get_all_futures_trades():
    """Holt alle Futures-Trades als Pandas DataFrame."""
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM futures_trades", conn)
    return df

if __name__ == "__main__":
    setup_database()