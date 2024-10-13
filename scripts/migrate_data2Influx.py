import os
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from tqdm import  tqdm

# Replace with your InfluxDB details
token = "40T00vQ7iLPfd28AkNdHR-UfdxNKz4qmq7MX3pu-vCPluIedNbybWp8viidj2lePdXyTprKUsASaOs9liZ8JoQ=="
org = "self"
bucket = "algotrade"
symbols_path = "src/config/stock_symbols.txt"
ticker_dir_path = "backups/TickerData"
orderbook_path = "backups/OrderBookData"

client = InfluxDBClient(url="http://localhost:8086", token=token)
write_api = client.write_api()


def load_symbols(symbols_file: str):
    """
    Load stock symbols from a file.
    :param symbols_file: Path to the file containing stock symbols.
    :return: List of stock symbols.
    """
    try:
        with open(symbols_file, 'r') as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"Symbols file not found: {symbols_file}")
        return []
    finally:
        if file:
            file.close()

symbols = load_symbols(symbols_path)

missing_symb = symbols
for symbol in tqdm(symbols):
    try:
        # Read ticker data CSV
        ticker_file_path = os.path.join(ticker_dir_path,f'{symbol}_ticker_data.csv')
        if not os.path.exists(ticker_file_path):  continue
        df_ticker = pd.read_csv(ticker_file_path)

        # Convert epoch_time to datetime if necessary
        df_ticker['symbol'] = symbol
        df_ticker['date'] = pd.to_datetime(df_ticker['date'])

        df_ticker.set_index('date', inplace=True)

        df_ticker = df_ticker[['symbol', 'open', 'high', 'low', 'close', 'volume']]

        # Write DataFrame to InfluxDB
        write_api.write(
            bucket=bucket,
            org=org,
            record=df_ticker,
            data_frame_measurement_name='ticker_data',
        )
    except:
        missing_symb.append(symbol)
write_api.close()


# # Repeat similar steps for order book data


# for  symbol in symbols:
#     # Read ticker data CSV
#     if not os.path.exists(os.path.join(ticker_dir_path,f'{symbol}_ticker_data.csv')):  continue
#     df_ticker = pd.read_csv(f'{symbol}_ticker_data.csv')

#     # Convert epoch_time to datetime if necessary
#     df_ticker['symbol'] = symbol
#     df_ticker['datetime'] = pd.to_datetime(df_ticker['epoch_time'], unit='s')

#     # Write data to InfluxDB
#     for _, row in df_ticker.iterrows():
#         point = Point("ticker_data") \
#             .tag("symbol", row['symbol']) \
#             .field("open", row['open']) \
#             .field("high", row['high']) \
#             .field("low", row['low']) \
#             .field("close", row['close']) \
#             .field("volume", row['volume']) \
#             .time(row['datetime'], WritePrecision.NS)
#         write_api.write(bucket=bucket, org=org, record=point)