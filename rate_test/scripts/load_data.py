"""
load_data.py

Module responsible for loading and preprocessing input datasets required
for the FX price conversion pipeline.

This includes:
- Reading the configuration-defined currency reference file
- Loading price and spot rate data
- Ensuring proper timestamp parsing
- Supporting both CSV and Parquet file types as specified in config.json
"""
import pandas as pd
from typing import Optional
from scripts.project_root import add_project_root
add_project_root()

from scripts.config import Config

class DataLoader:
    """
    Loads all required data files as specified in the Config object.

    Args:
        config (Config): An instance of the configuration class.

    Attributes:
        config (Config): Stores the configuration used to load data.
    """
    def __init__(self, config: Config) -> None:
        self.config = config

    def load_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Loads all data files (ccy, price, spot) and applies preprocessing where needed.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
                - Currency data
                - Price data with datetime-converted timestamps
                - Spot rate data with datetime-converted timestamps
        """
        ccy_df = self._load(self.config.ccy)
        price_df = self._load(self.config.price)
        spot_df = self._load(self.config.spot)

        # Retain only the necessary columns from price_df
        price_cols = [
        self.config.columns.timestamp,
        self.config.columns.price,
        self.config.columns.ccy_pair
        ]

        price_df = price_df[price_cols].copy()

        # Retain only the necessary columns from spot_df
        spot_cols = [
            self.config.columns.timestamp,
            self.config.columns.ccy_pair,
            self.config.columns.spot_rate
        ]
        spot_df = spot_df[spot_cols].copy()

        # Ensure timestamps are parsed to datetime
        ts_col = self.config.columns.timestamp
        ts_format = self.config.timestamp_format

        price_df[ts_col] = pd.to_datetime(price_df[ts_col], format=ts_format)
        spot_df[ts_col] = pd.to_datetime(spot_df[ts_col], format=ts_format)

        return ccy_df, price_df, spot_df

    def _load(self, cfg_data: "ConfigData") -> pd.DataFrame:
        """
        Generic method to load a file using its type and read_args.

        Args:
            cfg_data (ConfigData): Configuration object for the file to load.

        Returns:
            pd.DataFrame: Loaded data as a pandas DataFrame.
        """
        if cfg_data.type == "csv":
            return pd.read_csv(cfg_data.path, **cfg_data.read_args)
        elif cfg_data.type == "parquet":
            return pd.read_parquet(cfg_data.path, **cfg_data.read_args)
        else:
            raise ValueError(f"Unsupported file type: {cfg_data.type}")