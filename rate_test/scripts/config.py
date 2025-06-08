
import json
from pathlib import Path

class Config:
    """
    Loads and organizes configuration parameters from a JSON file.

    Args:
        config_path (Path): Path to the config.json file.

    Attributes:
        ccy (ConfigData): Configuration for the currency data file.
        price (ConfigData): Configuration for the price data file.
        spot (ConfigData): Configuration for the spot rate data file.
        output (ConfigData): Configuration for the output file.
    """
    def __init__(self, config_path: Path):
        config_path = config_path.resolve()
        with open(config_path, 'r') as f:
            config = json.load(f)

        base_dir = config_path.parent

        self.ccy = ConfigData(config["data"]["ccy_file"], base_dir)
        self.price = ConfigData(config["data"]["price_file"], base_dir)
        self.spot = ConfigData(config["data"]["spot_file"], base_dir)
        self.output = ConfigData(config["output_file"], base_dir)
        self.timestamp_format = config.get("timestamp_format", "%Y-%m-%d %H:%M:%S.%f")
        self.columns = ColumnConfig(config.get("columns", {}))

class ConfigData:
    """
    Represents a single data file configuration block.

    Args:
        cfg (dict): Dictionary containing 'path', 'type', and optional 'read_args'.
        base_dir (Path): Base directory relative to the config file.

    Attributes:
        path (Path): Absolute path to the file.
        type (str): File type, e.g., 'csv' or 'parquet'.
        read_args (dict): Optional arguments passed to pandas when reading the file.

    Returns:
        ConfigData: A configuration wrapper for a single file.
    """
    def __init__(self, cfg: dict, base_dir: Path):
        self.path = base_dir / cfg["path"]
        self.type = cfg["type"]
        self.read_args = cfg.get("read_args", {})

    def __repr__(self):
        """
        String representation of the ConfigData object for debugging.

        Returns:
            str: Readable string showing path, type and read_args.
        """
        return f"ConfigData(path={self.path}, type={self.type}, read_args={self.read_args})"
    
class ColumnConfig:
    def __init__(self, cfg: dict):
        self.timestamp = cfg.get("timestamp", "timestamp")
        self.price = cfg.get("price", "price")
        self.ccy_pair = cfg.get("ccy_pair", "ccy_pair")
        self.spot_rate = cfg.get("spot_rate", "spot_mid_rate")
        self.conversion_factor = cfg.get("conversion_factor", "conversion_factor")
        self.convert_price = cfg.get("convert_price", "convert_price")
