"""
price_converter.py

Contains a class for converting FX prices based on conversion rules and spot rates.

# Note: security_id is present in the dataset but is not used in conversion logic.
# The task requires a price per row, not per unique instrument or ID.
"""

import pandas as pd

class PriceConverter:
    def __init__(self, config, ccy_df: pd.DataFrame, price_df: pd.DataFrame, spot_df: pd.DataFrame):
        """
        Initializes the PriceConverter.

        Args:
            config (Config): Config object with column and format settings
            ccy_df (pd.DataFrame): Currency conversion rules
            price_df (pd.DataFrame): Price data to transform
            spot_df (pd.DataFrame): FX spot rate data
        """
        self.config = config
        self.ccy_df = ccy_df
        self.price_df = price_df.copy()
        self.spot_df = spot_df.copy()
        self.result_df = None

    def merge_conversion_info(self) -> None:
        """
        Merge conversion rules (whether to convert and conversion factor)
        into the price data based on ccy_pair.
        """
        ccy_pair_col = self.config.columns.ccy_pair

        self.price_df = self.price_df.merge(
            self.ccy_df,
            on=ccy_pair_col,
            how="left"
        )
        
        # --- Sanitize convert_price ---
        # After inspecting the data, all rows with convert_price == NaN also have conversion_factor == NaN.
        # This indicates those ccy_pairs are not supported for conversion.
        # Assumption: if convert_price is NaN, treat it as False (i.e., no conversion required).
        self.price_df[self.config.columns.convert_price] = (
            self.price_df[self.config.columns.convert_price].fillna(False).astype(bool)
        )

    def match_spot_rates(self) -> None:
        """
        For rows requiring conversion, find the most recent spot rate
        within the hour preceding the price timestamp.

        Uses pandas.merge_asof to join each price row with the most recent
        spot rate for the same ccy_pair within a 1-hour tolerance.
        """
        col = self.config.columns

        # Prepare spot DataFrame: sort by timestamp and pair
        self.spot_df = self.spot_df.sort_values(by=[col.timestamp, col.ccy_pair]).reset_index(drop=True)

        # Mask only rows that require conversion
        mask_convert = self.price_df[col.convert_price] == True
        to_merge = self.price_df[mask_convert].copy()
        to_merge["merge_index"] = to_merge.index  # Temporary index to realign post-merge
        to_merge = to_merge.sort_values(by=[col.timestamp, col.ccy_pair]).reset_index(drop=True)
      
        # Perform the asof merge to get the most recent spot rate within 1 hour
        matched = pd.merge_asof(
            to_merge,
            self.spot_df,
            by=col.ccy_pair,
            left_on=col.timestamp,
            right_on=col.timestamp,
            direction='backward',
            tolerance=pd.Timedelta("1h")
        )
        
        # Assign matched spot rates back into the main price_df
        self.price_df.loc[mask_convert, col.spot_rate] = matched[col.spot_rate].values

        col = self.config.columns

        col = self.config.columns

#       Initialize conversion status column
        self.price_df["conversion_status"] = "no_conversion_required"

        # Para filas con convert_price == True
        mask_convert = self.price_df[col.convert_price] == True

        # Marca si spot_rate está asignado
        mask_spot_assigned = self.price_df.loc[mask_convert, col.spot_rate].notna()

        # Actualiza estado según disponibilidad de spot_rate
        self.price_df.loc[mask_convert & mask_spot_assigned, "conversion_status"] = "conversion_done"
        self.price_df.loc[mask_convert & ~mask_spot_assigned, "conversion_status"] = "conversion_failed_no_spot_rate"

    def calculate_new_prices(self) -> None:
        """
        Vectorized calculation of new prices leveraging the 'conversion_status' column for efficiency.

        Logic:
        - For rows with conversion_status == 'no_conversion_required', new_price is the existing price.
        - For rows with conversion_status == 'conversion_done', new_price is calculated as (price / conversion_factor) + spot_mid_rate.
        - For rows with conversion_status == 'conversion_failed_no_spot_rate', new_price is set to NaN.

        This vectorized approach avoids row-wise iteration and is optimized for performance
        on large DataFrames.

        Args:
            None

        Returns:
            None. The method modifies self.price_df in place, adding the 'new_price' column.
        """
        col = self.config.columns
        #df = self.price_df.copy()

        # Start with existing price as default for all rows
        self.price_df["new_price"] = self.price_df[col.price]

        # Mask for rows where conversion was successfully done
        mask_conversion_done = self.price_df["conversion_status"] == "conversion_done"

        # Vectorized calculation of new prices where conversion is done
        self.price_df.loc[mask_conversion_done, "new_price"] = (
            self.price_df.loc[mask_conversion_done, col.price] / self.price_df.loc[mask_conversion_done, "conversion_factor"]
            + self.price_df.loc[mask_conversion_done, col.spot_rate]
        )

        # Mask for rows where conversion failed due to missing spot rate
        mask_conversion_failed = self.price_df["conversion_status"] == "conversion_failed_no_spot_rate"

        # Assign NaN to new_price where conversion failed
        self.price_df.loc[mask_conversion_failed, "new_price"] = float("nan")

    def export_results(self) -> None:
        """
        Save the DataFrame with new prices to output path.
        """
        path = self.config.output.path
        fmt = self.config.output.type
        args = self.config.output.read_args
        self.price_df.to_csv(path, index=False, **args)