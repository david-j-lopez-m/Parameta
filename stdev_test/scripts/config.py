import json
from pathlib import Path

class Config:
    def __init__(self, config_path: Path):
        config_path = config_path.resolve()
        with open(config_path, 'r') as f:
            config = json.load(f)

        base_dir = config_path.parent

        self.ccy = ConfigData(config["data"].get("ccy_file", {}), base_dir)
        self.price = ConfigData(config["data"].get("price_file", {}), base_dir)
        self.spot = ConfigData(config["data"].get("spot_file", {}), base_dir)
        self.stdev = ConfigData(config["data"].get("stdev_file", {}), base_dir)
        self.output = ConfigData(config.get("output_file", {}), base_dir)

        self.calc_params = config.get("calculation_params", {})
        self.start_calc = self.calc_params.get("start_calc", "2021-11-20 00:00:00")
        self.end_calc = self.calc_params.get("end_calc", "2021-11-23 09:00:00")
        self.date_format = self.calc_params.get("date_format", "%Y-%m-%d %H:%M:%S")
        self.timestamp_col = self.calc_params.get("timestamp_col", "snap_time")

class ConfigData:
    def __init__(self, cfg: dict, base_dir: Path):
        self.path = base_dir / cfg.get("path", "")
        self.type = cfg.get("type", "csv")
        self.read_args = cfg.get("read_args", {})

    def __repr__(self):
        return f"ConfigData(path={self.path}, type={self.type}, read_args={self.read_args})"