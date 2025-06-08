import pandas as pd

class Preprocessor:
    """
    Class responsible for preprocessing raw stdev price data to prepare it
    for rolling standard deviation calculation.

    Responsibilities include:
    - Converting timestamps.
    - Sorting data.
    - Filtering by time range.
    - Detecting contiguous hourly blocks without gaps.
    - Handling missing data if necessary.
    """

    def __init__(self, config):
        """
        Initialize with a config object.

        Args:
            config: Config object containing parameters such as date format and ranges.
        """
        self.config = config

    def convert_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        time_col = self.config.timestamp_col
        df[time_col] = pd.to_datetime(df[time_col], format=self.config.date_format)
        return df

    def sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        time_col = self.config.timestamp_col
        return df.sort_values(['security_id', time_col]).reset_index(drop=True)

    def filter_time_range(self, df: pd.DataFrame) -> pd.DataFrame:
        time_col = self.config.timestamp_col
        start_calc = pd.to_datetime(self.config.start_calc)
        end_calc = pd.to_datetime(self.config.end_calc)
        start_window = start_calc - pd.Timedelta(hours=20)
        return df[(df[time_col] >= start_window) & (df[time_col] <= end_calc)].copy()

    def detect_contiguous_blocks(self, df: pd.DataFrame) -> pd.DataFrame:
        time_col = self.config.timestamp_col
        df['time_diff'] = df.groupby('security_id')[time_col].diff()
        df['contig_block'] = (df['time_diff'] != pd.Timedelta(hours=1)).cumsum()
        return df

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        time_col = self.config.timestamp_col
        df = self.convert_timestamps(df)
        df = self.sort_data(df)
        df = self.filter_time_range(df)
        df = self.detect_contiguous_blocks(df)
        return df