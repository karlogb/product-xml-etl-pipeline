import os
import polars as pl


def present_product_xml_data(data_path):
    """
    Processes and presents product XML data by reading, filtering, and saving it as a Parquet file.
    Args:
        data_path (str): The base directory path where the product data is located and where the processed data will be saved.
    Returns:
        None
    The function performs the following steps:
    1. Reads the product data from Parquet files located in the specified data path.
    2. Drops the 'product_type_list' column and ensures the data is unique.
    3. Ranks the data by 'proc_date_key' in descending order within each 'mpn' group.
    4. Filters the data to keep only the highest-ranked entries for each 'mpn'.
    5. Saves the processed data to a new Parquet file in the 'present/product_data' directory within the specified data path.
    """
    parquet_file_path = f'{data_path}/preprocess/product_data/*/product_prep.parquet'
    product_data_load = pl.read_parquet(parquet_file_path).drop('product_type_list').unique()
    
    product_data = \
        product_data_load \
            .with_columns(pl.col("proc_date_key").rank(method="ordinal", descending=True).over(pl.col("mpn")).alias("rank")) \
            .filter(pl.col("rank") == 1) \
            .drop("rank")
    
    save_path = f"{data_path}/present/product_data"
    os.makedirs(save_path, exist_ok=True)
    parquet_file_path = f"{save_path}/product_data.parquet"
    product_data.write_parquet(parquet_file_path)


def main():
    """
    Main function to present product XML data.
    This function sets the data path to the location of the web data and calls the
    `present_product_xml_data` function to process and present the XML data.
    Args:
        None
    Returns:
        None
    """
    data_path = "/home/data/"

    present_product_xml_data(data_path)


if __name__ == "__main__":
    main()
