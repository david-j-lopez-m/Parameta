from pathlib import Path
import sys
import pandas as pd
import time

# Añadir root del proyecto al path para importar scripts
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))

from scripts.config import Config
from scripts.load_data import StdevDataLoader
from scripts.preprocessing import Preprocessor
from scripts.stdev_calculator import StdevCalculator

def main():

    # Uncomment to measure time elapsed
    start_time = time.perf_counter()

    # Load config
    config = Config(Path("config.json"))

    # Load data
    loader = StdevDataLoader(config)
    df_raw = loader.load_data()

    # Preprocess data
    preprocessor = Preprocessor(config)
    df_preprocessed = preprocessor.preprocess(df_raw)

    # Calcular rolling std
    calculator = StdevCalculator(window_size=20)
    df_result = calculator.calculate_rolling_std(df_preprocessed)

    # Filter final range (from start_calc to end_calc)
    start_result = pd.to_datetime(config.start_calc)
    end_result = pd.to_datetime(config.end_calc)
    df_final = df_result[(df_result['snap_time'] >= start_result) & (df_result['snap_time'] <= end_result)].copy()

    # Save result
    output_cfg = config.output
    df_final.to_csv(output_cfg.path, index=False, **output_cfg.read_args)

    # Uncomment to measure time elapsed
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(f"Total execution time: {elapsed:.3f} seconds")

if __name__ == "__main__":
    main()