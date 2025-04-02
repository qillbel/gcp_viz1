from google.cloud import bigquery, storage
import pandas as pd
import os

# Set your parameters
PROJECT_ID = "dashgcp-452719"
DATASET_ID = "qb_ordsur_1_dset"
TABLE_ID = "tab_OpenNames"
BUCKET_NAME = "qb_viz_1"
CSV_FILENAME = "to_tableau.csv"
DESTINATION_BLOB_NAME = f"tableau_exports/{CSV_FILENAME}"

# Authenticate
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/dashgcp-452719-e4527addb2a3.json"

def query_bigquery():
    """Runs a query and returns the results as a Pandas DataFrame."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` ORDER BY RANDOM() LIMIT 50"
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df

def save_to_csv(df):
    """Saves DataFrame to a local CSV file."""
    df.to_csv(CSV_FILENAME, index=False)
    print(f"CSV file saved: {CSV_FILENAME}")

def upload_to_gcs():
    """Uploads the CSV file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB_NAME)
    blob.upload_from_filename(CSV_FILENAME)
    print(f"File uploaded to GCS: gs://{BUCKET_NAME}/{DESTINATION_BLOB_NAME}")

def main():
    df = query_bigquery()
    save_to_csv(df)
    upload_to_gcs()

if __name__ == "__main__":
    main()
