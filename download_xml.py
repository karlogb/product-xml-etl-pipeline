import os
import requests
from datetime import datetime
from pytz import timezone


def download_xml_data(name, url, save_folder, run_date_key):
    """
    Downloads XML data from a given URL and saves it to a specified folder.
    Args:
        name (str): The name to be used for the saved XML file.
        url (str): The URL from which to download the XML data.
        save_folder (str): The folder where the XML file should be saved.
        run_date_key (str): A key representing the run date, used to create a subfolder for saving the file.
    Raises:
        Exception: If the download fails (i.e., the response status code is not 200).
    Returns:
        None
    """
    response = requests.get(url)
    
    if response.status_code == 200:
        xml_data = response.content
        save_path = f"{save_folder}/{run_date_key}"
        os.makedirs(save_path, exist_ok=True)
        
        with open(f"{save_path}/{name}_raw.xml", 'wb') as xml_file:
            xml_file.write(xml_data)

        return print(f"{name} LANDING: DONE with date: {run_date_key}")
    else:
        raise Exception(f"Failed to download data. Status code: {response.status_code}")

def main():
    """
    Main function to download XML data from a specified URL and save it to a designated folder.
    This function performs the following steps:
    1. Generates a run date key based on the current date and time in the 'Europe/Bratislava' timezone.
    2. Defines the URL of the XML data to be downloaded.
    3. Specifies the name of the product and the user folder path.
    4. Constructs the save folder path by appending the product data directory to the user folder path.
    5. Calls the download_xml_data function with the specified parameters to download and save the XML data.
    Returns:
        None
    """
    run_date_key = datetime.now(timezone('Europe/Bratislava')).strftime('%Y%m%d')
    url = ''
    name = "product"
    user_folder = "/home/data/"
    save_folder =  user_folder + "/web/landing/product_data"

    download_xml_data(name, url, save_folder, run_date_key)


if __name__ == "__main__":
    main()
