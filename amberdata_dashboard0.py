import os
import json
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, timezone
from amberdata_rest.futures import FuturesRestService
from amberdata_rest.common import ApiKeyGetMode
from amberdata_rest.constants import MarketDataVenue, TimeInterval, TimeFormat

# --- Page Setup ---
st.set_page_config(page_title="Amberdata Dashboard", layout="wide")
st.sidebar.title("Amberdata Tools")
selected_tool = st.sidebar.radio("Choose a Tool", [
    "Futures OHLCV",
    "Funding Rates",
    "Order Book Explorer",
    "Liquidations (coming soon)",
    "Missing Dates Checker (coming soon)"
])

# --- API Key Setup ---
api_file = st.sidebar.text_input("API Key JSON file path", value="keys.json")
service = None

if os.path.exists(api_file):
    try:
        with open(api_file, "r") as f:
            keys = json.load(f)
        st.sidebar.success("✅ JSON loaded successfully")
        st.sidebar.write(f"🔑 Using local_key_path: `{keys.get('local_key_path')}`")
        service = FuturesRestService(
            api_key_get_mode=ApiKeyGetMode.LOCAL_FILE,
            api_key_get_params={"local_key_path": keys["local_key_path"]}
        )
    except Exception as e:
        st.sidebar.error(f"❌ Could not load API key: {e}")
else:
    st.sidebar.error("❌ API key file not found.")

# --- Tool 1: Futures OHLCV ---
if selected_tool == "Futures OHLCV":
    st.title("📈 Futures OHLCV Explorer")
    symbol = st.text_input("Symbol", "BTCUSD_PERP")
    exchange = st.text_input("Exchange", "binance")
    interval = st.selectbox("Interval", ["1m", "1h", "1d"])
    start_date = st.date_input("Start Date", datetime(2024, 1, 1))
    end_date = st.date_input("End Date", datetime(2024, 1, 7))

    interval_map = {
        "1m": TimeInterval.MINUTE,
        "1h": TimeInterval.HOUR,
        "1d": TimeInterval.DAY
    }

    if st.button("Fetch OHLCV"):
        try:
            result_df = service.get_ohlcv(
                instrument=symbol,
                exchanges=[MarketDataVenue(exchange)],
                start_date=start_date,
                end_date=end_date,
                time_interval=interval_map[interval]
            )
            st.dataframe(result_df.reset_index())
            st.download_button("Download CSV", result_df.reset_index().to_csv(index=False), file_name=f"{symbol}_ohlcv.csv")
        except Exception as e:
            st.error(f"Error fetching OHLCV: {e}")

# --- Tool 2: Funding Rates ---
elif selected_tool == "Funding Rates":
    st.title("💸 Funding Rates Tool")
    symbol = st.text_input("Symbol", "BTCUSD_PERP", key="funding_symbol")
    exchange = st.text_input("Exchange", "binance", key="funding_exchange")
    start_date = st.date_input("Start Date", datetime(2024, 1, 1), key="funding_start")
    end_date = st.date_input("End Date", datetime(2024, 1, 7), key="funding_end")

    if st.button("Fetch Funding Rates"):
        try:
            start_dt = datetime.combine(start_date, datetime.min.time())
            end_dt = datetime.combine(end_date, datetime.min.time())
            result_df = service.get_funding_rates(
                instrument=symbol,
                exchange=MarketDataVenue(exchange),
                start_date=start_dt,
                end_date=end_dt
            )
            if result_df is not None and not result_df.empty:
                st.dataframe(result_df.reset_index(drop=True))
                st.download_button("Download CSV", result_df.to_csv(index=False), file_name=f"{symbol}_funding.csv")
            else:
                st.warning("No funding rate data returned for the given range.")
        except Exception as e:
            st.error(f"Error fetching funding rates: {e}")

# --- Tool 3: Order Book Explorer ---
elif selected_tool == "Order Book Explorer":
    st.header("Order Book Explorer")
    if not service:
        st.error("FuturesRestService not initialized.")
    else:
        instrument = st.text_input("Instrument", "BTCUSDT")
        exchange = st.selectbox("Exchange", [e.name for e in MarketDataVenue], index=0)
        now = datetime.now(timezone.utc)

        start_date = st.date_input("Start Date", now.date())
        start_time_val = st.time_input("Start Time", (now - timedelta(minutes=5)).time())
        end_date = st.date_input("End Date", now.date())
        end_time_val = st.time_input("End Time", now.time())

        start_time = datetime.combine(start_date, start_time_val).replace(tzinfo=timezone.utc)
        end_time = datetime.combine(end_date, end_time_val).replace(tzinfo=timezone.utc)

        def get_snapshot():
            df = service.get_order_book_snapshots_historical(
                instrument=instrument,
                exchange=MarketDataVenue[exchange],
                start_date=start_time,
                end_date=end_time,
                time_format=TimeFormat.MILLISECONDS
            )
            return df

        def get_events(start_time, end_time):
            df = service.get_order_book_events_historical(
                instrument=instrument,
                exchange=MarketDataVenue[exchange],
                start_date=start_time,
                end_date=end_time,
                time_format=TimeFormat.MILLISECONDS
            )
            return df

        def apply_events(snapshot, events):
            if 'sequence' in snapshot.columns and 'sequence' in events.columns:
                snapshot_seq = snapshot['sequence'].max()
                events = events[events['sequence'] > snapshot_seq]
            else:
                snap_ts_ns = snapshot['timestamp'].max() * 1_000_000 + snapshot['timestampNanoseconds'].max()
                events['event_ts_ns'] = events['timestamp'] * 1_000_000 + events['timestampNanoseconds']
                events = events[events['event_ts_ns'] > snap_ts_ns]

            book = snapshot.copy()
            for _, event in events.iterrows():
                for side in ['bid', 'ask']:
                    for level in event.get(side, []):
                        price = level.get("price")
                        volume = level.get("volume")
                        mask = (book["side"] == side) & (book["price"] == price)

                        if volume == 0:
                            book = book[~mask]
                        elif mask.any():
                            book.loc[mask, ["volume"]] = volume
                        else:
                            new_row = {**event.to_dict(), **level, "side": side}
                            book = pd.concat([book, pd.DataFrame([new_row])], ignore_index=True)

            return book.reset_index(drop=True)

        if st.button("Get Snapshot"):
            snapshot = get_snapshot()
            if snapshot.empty:
                st.warning("No snapshot found in that range.")
            else:
                if 'exchangeTimestamp' in snapshot.columns and 'timestamp' not in snapshot.columns:
                    snapshot = snapshot.rename(columns={'exchangeTimestamp': 'timestamp'})
                if 'timestamp' in snapshot.columns:
                    snapshot['timestamp'] = pd.to_datetime(snapshot['timestamp'], unit='ms', utc=True)
                st.session_state['order_book'] = snapshot
                st.dataframe(snapshot)

        if 'order_book' in st.session_state:
            st.subheader("Current Order Book")
            st.dataframe(st.session_state['order_book'])

            if st.button("Apply New Events"):
                order_book_df = st.session_state['order_book']
                if 'timestamp' not in order_book_df.columns:
                    st.error("❌ 'timestamp' column not found in the order book snapshot. Please check your snapshot data format.")
                else:
                    last_ts = pd.to_datetime(order_book_df['timestamp'].max(), utc=True)
                    new_events = get_events(last_ts, datetime.now(timezone.utc))
                    if new_events.empty:
                        st.info("No new events since last snapshot.")
                    else:
                        updated_book = apply_events(order_book_df, new_events)
                        st.session_state['order_book'] = updated_book
                        st.dataframe(updated_book)

# --- Tool 4: Liquidations ---
elif selected_tool == "Liquidations (coming soon)":
    st.title("🚨 Liquidations Viewer")
    st.warning("Under construction – will fetch liquidation data per symbol and exchange.")

# --- Tool 5: Missing Dates Checker ---
elif selected_tool == "Missing Dates Checker (coming soon)":
    st.title("🧩 Missing Date Checker")
    st.warning("Under construction – will check for gaps in historical OHLCV data.")















