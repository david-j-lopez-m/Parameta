import pandas as pd
import pandas as pd

class StdevCalculator:
    """
    Class to calculate rolling standard deviation of bid, mid, and ask prices
    for each security_id, given preprocessed data with contiguous hourly blocks.
    """

    def __init__(self, window_size: int = 20):
        """
        Initialize with window size for rolling calculation.

        Args:
            window_size (int): Size of rolling window in hours.
        """
        self.window_size = window_size

    def calculate_rolling_std(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate rolling standard deviations per security_id and contiguous block.

        Args:
            df (pd.DataFrame): Preprocessed DataFrame including 'contig_block'.

        Returns:
            pd.DataFrame: DataFrame with added columns:
                - 'bid_stdev'
                - 'mid_stdev'
                - 'ask_stdev'
        """
        result = df.groupby(['security_id', 'contig_block']).apply(
            lambda x: x.assign(
                bid_stdev = x['bid'].rolling(window=self.window_size, min_periods=self.window_size).std(),
                mid_stdev = x['mid'].rolling(window=self.window_size, min_periods=self.window_size).std(),
                ask_stdev = x['ask'].rolling(window=self.window_size, min_periods=self.window_size).std(),
            )
        ).reset_index(drop=True)
        return result