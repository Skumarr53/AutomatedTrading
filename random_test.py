import os
import pyarrow.parquet as pq

def check_data_length_consistency(folder_path):
    lengths = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(folder_path, file_name)
            table = pq.read_table(file_path)
            length = len(table)
            lengths.append(length)
            print(file_name, length)
    
    if len(set(lengths)) == 1:
        print("All Parquet files have data of the same length:", lengths[0])
    else:
        print("Parquet files have varying data lengths:", lengths)


# Example usage
folder_path = "/home/skumar/DataScience/Projects/AlgoTrading_datacollection/backups/TickerData"
check_data_length_consistency(folder_path)