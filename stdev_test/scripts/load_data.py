import pandas as pd

class StdevDataLoader:
    """
    Class to load and inspect stdev price data for rolling standard deviation calculation.

    Responsibilities:
    - Load data from parquet file based on config.
    - Convert timestamps to datetime.
    - Provide a simple data inspection method to check shape, columns, NaNs, and stats.
    """

    def __init__(self, config):
        """
        Initialize the loader with a config object.

        Args:
            config: Config object with file paths and read parameters.
        """
        self.config = config

    def load_data(self):
        """
        Load the stdev_price_data parquet file as a pandas DataFrame.

        Returns:
            pd.DataFrame: Loaded data with timestamps converted to datetime.
        """
        file_cfg = self.config.stdev
        if file_cfg.type == "parquet":
            df = pd.read_parquet(file_cfg.path, **file_cfg.read_args)
        else:
            raise NotImplementedError("Only parquet files supported for stdev data.")

        return df