import os
import pandas as pd
from tqdm import tqdm

def convert_parquet_to_csv(input_dir, output_dir):
    """
    Convert all Parquet files in the input directory to CSV files in the output directory.

    Args:
        input_dir (str): Directory containing Parquet files.
        output_dir (str): Directory to save converted CSV files.

    Returns:
        None
    """
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # List all files in the input directory
    for filename in tqdm(os.listdir(input_dir)):
        if filename.endswith('.parquet'):
            parquet_file_path = os.path.join(input_dir, filename)
            csv_file_name = filename.replace('.parquet', '.csv')
            csv_file_path = os.path.join(output_dir, csv_file_name)

            # Read the Parquet file into a DataFrame
            try:
                df = pd.read_parquet(parquet_file_path)
                # Save the DataFrame as a CSV file
                df.to_csv(csv_file_path, index=False)
                print(f"Converted: {filename} to {csv_file_name}")
            except Exception as e:
                print(f"Error converting {filename}: {e}")

# Usage

if __name__ == '__main__':
    input_directory = 'backups/TickerData'  # Replace with the path to your input directory
    output_directory = 'backups/TickerData'  # Replace with the path to your output directory

    convert_parquet_to_csv(input_directory, output_directory)
