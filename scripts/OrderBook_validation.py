import os, sys
from  pathlib import Path
import logging
from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
import pyarrow.parquet as pq
import holidays
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from src.config import config
from dataclasses import dataclass

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class ValidationConfig:
    input_directory: str = config.ORDERBOOK_FILENAME
    output_directory: str = "WeeklyReports/orderbook_weekly_valid_plots"
    holiday_year: int = datetime.now().year

class CsvValidator:
    """Class to validate csv files for missing trading intervals."""

    def __init__(self, config: ValidationConfig):
        self.config = config
        self.indian_holidays = holidays.India(years=self.config.holiday_year)
        self.intervals = pd.date_range(start="09:15", end="16:00", freq="5T").time
        self.trading_days = self._get_trading_days()

        # Create the output directory if it doesn't exist
        os.makedirs(self.config.output_directory, exist_ok=True)

    def _get_trading_days(self) -> pd.DatetimeIndex:
        """Generate trading days for the current week, excluding holidays."""
        today = datetime.now().date()
        current_week_monday = today - timedelta(days=today.weekday())
        custom_bd = CustomBusinessDay(holidays=self.indian_holidays)
        return pd.bdate_range(start=current_week_monday, end=today, freq=custom_bd)

    #EDITED: Updated heatmap generation to include missing percentages alongside day labels
    def _generate_heatmap(self, validation_matrix: np.ndarray, missing_percentage: List[float], file_name: str) -> None:
        """Generate and save a heatmap for missing intervals."""
        
        # Combine day names with their missing percentages
        yticklabels = [f"{day.strftime('%A')} ({perc:.0f}%)" for day, perc in zip(self.trading_days, missing_percentage)]
        
        plt.figure(figsize=(12, 7))  # Slightly increase the size for better readability
        sns.heatmap(validation_matrix, cmap='coolwarm_r', cbar=False,  # Reverse the colormap
                    yticklabels=yticklabels,
                    xticklabels=[interval.strftime('%H:%M') for interval in self.intervals],
                    linewidths=0.5, linecolor='white',  # Add gridlines for better separation
                    annot=True, fmt='',  # Add annotations for better clarity
                    square=True)  # Ensure each cell is square-shaped

        plt.title(f"Missing Time Slots for {Path(file_name).stem}", fontsize=16, weight='bold')
        plt.xlabel('Time Intervals', fontsize=12)
        plt.ylabel('Day of the Week', fontsize=12)
        
        plt.xticks(rotation=90)  # Rotate x-ticks for better readability
        plt.yticks(rotation=0)  # Keep y-ticks horizontal
        
        #EDITED: Removed the previous percentage text annotation
        # plt.figtext(...) line removed
        
        # Save the heatmap
        heatmap_file = os.path.join(self.config.output_directory, f"{Path(file_name).stem}_heatmap.png")
        plt.savefig(heatmap_file, bbox_inches='tight', dpi=300)  # Increase DPI for better quality
        plt.close()
        logging.info(f"Heatmap generated: {heatmap_file}")

    #EDITED: Removed combined heatmap generation as it's not required
    # If needed, similar changes can be applied to the combined heatmap method

    def _validate_file(self, file_path: str, combined_missing: np.ndarray) -> None:
        """Validate a single csv file for missing intervals."""
        print(file_path)
        try:
            df = pd.read_csv(file_path)
            df['last_traded_time'] = pd.to_datetime(df['last_traded_time'])
        except Exception as e:
            logging.error(f"Failed to process file {file_path}: {e}")
            return

        validation_matrix = np.ones((len(self.trading_days), len(self.intervals)))
        existing_percentage = []

        for i, day in enumerate(self.trading_days):
            day_data = df[df['last_traded_time'].dt.date == day.date()]
            if not day_data.empty:
                existing_intervals = set(day_data['last_traded_time'].dt.time)
                missing_intervals = set(self.intervals) - existing_intervals
                existing_count = len(existing_intervals)
                total_intervals = len(self.intervals)
                percentage_existing = (existing_count / total_intervals) * 100
                existing_percentage.append(percentage_existing)
                for j, interval in enumerate(self.intervals):
                    if interval in missing_intervals:
                        validation_matrix[i, j] = 0  # Mark missing intervals as 0
                        combined_missing[j] +=1  # Accumulate missing for combined plot
            else:
                existing_percentage.append(0.0)
                validation_matrix[i, :] = 0
                combined_missing +=1

        self._generate_heatmap(validation_matrix, existing_percentage, file_path)

    def generate_weekly_report(self) -> None:
        """Validate all csv files in the directory and generate heatmaps."""
        files = [f for f in Path(self.config.input_directory).iterdir() if f.suffix == '.csv']
        if not files:
            logging.warning(f"No csv files found in directory {self.config.input_directory}")
            return

        #EDITED: Initialize combined missing data array (if combined heatmap is needed)
        combined_missing = np.zeros(len(self.intervals))

        for file_path in files:
            self._validate_file(file_path, combined_missing)

        #EDITED: Removed combined heatmap generation as per latest requirement
        # If needed, calculate combined_percentage and generate combined_heatmap

def main() -> None:
    """Main function to run the csv validator."""
    config = ValidationConfig()
    validator = CsvValidator(config)
    validator.generate_weekly_report()

if __name__ == "__main__":
    main()
