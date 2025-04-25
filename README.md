# Product XML ETL Pipeline

This project implements a simple ETL pipeline in Python that downloads product data from an XML feed, preprocesses it using [Polars](https://www.pola.rs/), and generates a deduplicated output dataset containing the latest product entries.

## 📦 Structure

The pipeline consists of three scripts:

| Script                  | Description                                        |
|------------------------|----------------------------------------------------|
| `download_xml.py`      | Downloads the XML file and stores it in a raw data folder. |
| `preprocess_products.py` | Parses and processes the XML into a structured Parquet format. |
| `present_latest.py`    | Deduplicates and filters the data to keep only the most recent records per product. |

## 🧰 Requirements

- Python 3.8+
- [Polars](https://pola-rs.github.io/polars/py-polars/html/)
- pytz

Install dependencies:

```bash
pip install polars pytz
```

## 🚀 How to run

Make sure the directory structure looks like this:

```
project-root/
│
├── download_xml.py
├── preprocess_products.py
├── present_latest.py
├── data/
│   ├── landing/
│   ├── preprocess/
│   └── present/
```

Each script is standalone and can be run directly from the command line:

```bash
python download_xml.py
python preprocess_products.py
python present_latest.py
```

## 📁 Output

- Raw XML files: `data/landing/<YYYYMMDD>/product_raw.xml`
- Processed Parquet files: `data/preprocess/<YYYYMMDD>/product_prep.parquet`
- Final presented file: `data/present/product_data.parquet`

## 🛡️ Notes

- Paths are relative and safe to run across different environments.
- The scripts automatically create necessary folders if they don’t exist.
- Ideal for simple product feed tracking or data integration tasks.

---
