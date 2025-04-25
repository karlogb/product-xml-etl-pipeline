import xml.etree.ElementTree as ET
import polars as pl
from datetime import datetime
from pytz import timezone
import os


def preprocess_product_xml_data(landing_folder, save_folder, run_date_key):
    """
    Preprocess product XML data and save it as a Parquet file.
    This function reads raw XML data from a specified landing folder, parses the XML,
    extracts relevant data, processes it into a Polars DataFrame, and saves the processed
    data as a Parquet file in the specified save folder.
    Args:
        landing_folder (str): The folder where the raw XML data is located.
        save_folder (str): The folder where the processed data will be saved.
        run_date_key (str): A key representing the run date, used to locate the specific
                            subfolder and to name the output file.
    Raises:
        Exception: If the raw XML file is not found in the specified landing folder.
        Exception: If there is an error parsing the XML file.
    Returns:
        None
    """
    try:
        name = "product"
        landing_run_date_folder = f"{landing_folder}/{run_date_key}"
        
        with open(f"{landing_run_date_folder}/{name}_raw.xml", 'rb') as xml_file:
            xml_data = xml_file.read()

        # Parse XML data
        tree = ET.ElementTree(ET.fromstring(xml_data))
        root = tree.getroot()

        # Define the namespace
        namespace = {'g': 'http://base.google.com/ns/1.0'}
        
        data_list = []
        
        # Iterate through XML elements and extract data
        for item_elem in root.findall('.//channel/item', namespaces=namespace):
            data_dict = {}
            for child_elem in item_elem:
                col_name = child_elem.tag.replace('{http://base.google.com/ns/1.0}', '')
                data_dict[col_name] = child_elem.text
            data_list.append(data_dict)
        
        # Ensure that all dictionaries have the "sale_price" key
        for data_dict in data_list:
            data_dict.setdefault('sale_price', None)

        #Polars Dataframe
        product_prep = pl.DataFrame(data_list)\
            .with_columns(pl.col("product_type").str.split(">").alias("product_type_list"))\
            .with_columns(pl.col("product_type_list").map_elements(lambda lst: lst[0]).alias("product_type_cat"))\
            .with_columns(pl.col("product_type_list").map_elements(lambda lst: lst[1] if len(lst) > 1 else None).alias("product_type_subcat"))\
            .with_columns(proc_date_key = pl.lit(run_date_key))

        #Save
        save_path = f"{save_folder}/{run_date_key}"
        os.makedirs(save_path, exist_ok=True)
        parquet_file_path = f"{save_path}/{name}_prep.parquet"
        product_prep.write_parquet(parquet_file_path)

        print(f"{name} PREPROCESS: DONE with date: {run_date_key}")

    except FileNotFoundError:
        raise Exception(f"File not found: {landing_run_date_folder}")
    except ET.ParseError:
        raise Exception(f"Failed to parse XML file: {landing_run_date_folder}")

def main():
    """
    Main function to preprocess product XML data.
    This function generates a run date key based on the current date and time in the 'Europe/Bratislava' timezone.
    It defines the user folder, landing folder, and save folder paths.
    Then, it calls the preprocess_product_xml_data function with the landing folder, save folder, and run date key as arguments.
    Args:
        None
    Returns:
        None
    """
    run_date_key = datetime.now(timezone('Europe/Bratislava')).strftime('%Y%m%d')
    user_folder = "data"
    landing_folder = user_folder + "/web/landing/product_data"
    save_folder = user_folder + '/web/preprocess/product_data'

    preprocess_product_xml_data(landing_folder, save_folder, run_date_key)


if __name__ == "__main__":
    main()
