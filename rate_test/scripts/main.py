# scripts/main.py

from pathlib import Path
import sys
import time

print("sys.path:")
for p in sys.path:
    print("   ", p)

# Add project root to sys.path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))

from scripts.config import Config
from scripts.load_data import DataLoader
from scripts.price_converter import PriceConverter

def main():
    """
    Main entry point to run the FX price conversion pipeline.
    Loads config, reads data, runs transformations, and saves results.
    """
    start_time = time.perf_counter()

    config = Config(Path("config.json"))
    loader = DataLoader(config)
    ccy_df, price_df, spot_df = loader.load_all()

    converter = PriceConverter(config, ccy_df, price_df, spot_df)
    converter.merge_conversion_info()
    converter.match_spot_rates()
    converter.calculate_new_prices()
    converter.export_results()

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(f"Total execution time: {elapsed:.3f} seconds")

if __name__ == "__main__":
    main()