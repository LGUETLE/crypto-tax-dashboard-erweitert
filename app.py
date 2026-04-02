import streamlit as st
import pandas as pd
import plotly.express as px
import ccxt
from db_manager import get_all_transactions, get_all_futures_trades

# --- KONFIGURATION ---
st.set_page_config(page_title="Crypto Quant Dashboard", layout="wide")

@st.cache_data(ttl=600)
def fetch_live_prices(assets):
    """Holt aktuelle Preise über CCXT (Bitvavo Public API)"""
    exchange = ccxt.bitvavo()
    prices = {"EUR": 1.0}
    for asset in assets:
        if asset == "EUR": continue
        try:
            ticker = exchange.fetch_ticker(f"{asset}/EUR")
            prices[asset] = ticker['last']
        except:
            prices[asset] = 0.0
    return prices

def calculate_balances(df):
    """Berechnet die aktuellen Bestände pro Asset"""
    balances = {}
    if df.empty: return balances
    
    for _, row in df.iterrows():
        asset = row['asset']
        amount = row['amount']
        tx_type = row['tx_type']
        
        if asset not in balances: balances[asset] = 0.0
        if tx_type in ['buy', 'deposit', 'staking', 'bonus']:
            balances[asset] += amount
        elif tx_type in ['sell', 'withdrawal']:
            balances[asset] -= amount
            
        if row['fee_amount'] > 0 and row['fee_asset'] == asset:
            balances[asset] -= row['fee_amount']
            
    return {k: v for k, v in balances.items() if v > 0.000001 and k != 'EUR'}

# --- DASHBOARD UI ---
st.title("🚀 Crypto Quant Dashboard")

# Erstelle die Tabs (Reiter)
tab_spot, tab_futures = st.tabs(["📈 Spot Portfolio (Bison & Bitvavo)", "🤖 Bot Futures (Bitget/Hyp)"])

# ==========================================
# TAB 1: SPOT PORTFOLIO
# ==========================================
with tab_spot:
    df_spot = get_all_transactions()
    
    if df_spot.empty:
        st.warning("Noch keine Spot-Daten in der Datenbank gefunden.")
    else:
        balances = calculate_balances(df_spot)
        unique_assets = list(balances.keys())
        
        with st.spinner('Lade Live-Preise für Spot...'):
            current_prices = fetch_live_prices(unique_assets)
        
        portfolio_data = []
        total_value_eur = 0
        
        for asset, amount in balances.items():
            price = current_prices.get(asset, 0)
            value_eur = amount * price
            total_value_eur += value_eur
            portfolio_data.append({
                "Asset": asset,
                "Bestand": round(amount, 6),
                "Preis (EUR)": f"{price:,.2f} €",
                "Wert (EUR)": round(value_eur, 2)
            })
        
        portfolio_df = pd.DataFrame(portfolio_data)

        st.metric(label="Gesamtportfolio Wert (Spot)", value=f"{total_value_eur:,.2f} €")

        col1, col2 = st.columns([1, 1])
        with col1:
            if not portfolio_df.empty and total_value_eur > 0:
                fig = px.pie(portfolio_df, values='Wert (EUR)', names='Asset', hole=0.4,
                             color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Keine Bestände zum Visualisieren.")

        with col2:
            st.dataframe(portfolio_df, use_container_width=True, hide_index=True)

# ==========================================
# TAB 2: FUTURES & BOT TRADING
# ==========================================
with tab_futures:
    df_futures = get_all_futures_trades()
    
    if df_futures.empty:
        st.info("Dein Bot hat noch keine Futures-Trades ausgeführt. Sobald er startet, tauchen die PnL-Daten hier auf!")
        st.image("https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?auto=format&fit=crop&w=800&q=80", width=400) # Platzhalter-Bild
    else:
        # PnL (Gewinn/Verlust) berechnen
        total_pnl = df_futures['realized_pnl'].sum()
        total_fees = df_futures['funding_fee'].sum()
        win_rate = (len(df_futures[df_futures['realized_pnl'] > 0]) / len(df_futures)) * 100 if len(df_futures) > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Realized PnL (Gesamt)", f"{total_pnl:,.2f} USDT")
        col2.metric("Gezahlte Funding Fees", f"{total_fees:,.2f} USDT")
        col3.metric("Win Rate", f"{win_rate:.1f} %")
        
        st.subheader("Letzte Bot-Trades")
        # Zeige die wichtigsten Spalten chronologisch an
        display_df = df_futures[['timestamp', 'exchange', 'symbol', 'position_side', 'leverage', 'realized_pnl']]
        st.dataframe(display_df.sort_values(by='timestamp', ascending=False), use_container_width=True, hide_index=True)

st.sidebar.info("Datenquelle: SQLite DB + Bitvavo Live API")
if st.sidebar.button("Daten neu laden"):
    st.cache_data.clear()
    st.rerun()