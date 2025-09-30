import os
import sys
import logging
import requests
from google.cloud import storage
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# WORKAROUND: Prevent timeout for large files, comment out if running for green datasets
# storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
# storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

# Configuration from environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DATASET_TYPE = os.getenv("DATASET_TYPE", "yellow")
YEARS = os.getenv("YEARS", "2024,2025").split(",")

# Batch configuration
START_MONTH = int(os.getenv("START_MONTH", "1"))
END_MONTH = int(os.getenv("END_MONTH", "12"))

# Base URL for NYC taxi data
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def upload_to_gcs(local_path, gcs_path):
    """Upload local file to GCS with timeout handling."""
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        # Upload with timeout (10 minutes for large files)
        blob.upload_from_filename(local_path, timeout=600)
        logging.info(f"Uploaded to gs://{BUCKET_NAME}/{gcs_path}")
        
        return f"gs://{BUCKET_NAME}/{gcs_path}"
    
    except Exception as e:
        logging.error(f"Failed to upload to GCS: {str(e)}")
        raise


def download_and_process_file(year, month, dataset_type):
    """Download file from URL and upload to GCS."""
    # Format month with zero padding
    month_str = f"{month:02d}"
    file_name = f"{dataset_type}_tripdata_{year}-{month_str}.parquet"
    
    try:
        url = f"{BASE_URL}/{file_name}"
        logging.info(f"Downloading {file_name} from {url}")
        
        # Download file with streaming
        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()
        
        # Save locally
        local_path = f"/tmp/{file_name}"
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logging.info(f"Local: {file_name}")
        
        # Upload to GCS with folder structure: {dataset_type}/{file_name}
        gcs_path = f"{dataset_type}/{file_name}"
        upload_to_gcs(local_path, gcs_path)
        logging.info(f"GCS: {gcs_path}")
        
        # Clean up local file
        os.remove(local_path)
        logging.info(f"Successfully processed {file_name}")
        return True
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.warning(f"File not found (404): {url}")
            return False
        else:
            logging.error(f"Error downloading: {e}")
            return False
    except Exception as e:
        logging.error(f"Failed to process {file_name}: {str(e)}")
        return False

def download_taxi_zone_lookup():
    """Download taxi zone lookup file and upload to GCS."""
    file_name = "taxi_zone_lookup.csv"
    url = f"https://d37ci6vzurychx.cloudfront.net/misc/{file_name}"
    
    try:
        logging.info(f"Downloading {file_name} from {url}")
        
        # Download file with streaming
        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()
        
        # Save locally
        local_path = f"/tmp/{file_name}"
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logging.info(f"Local: {file_name}")
        
        # Upload to GCS with folder structure: misc/{file_name}
        gcs_path = f"{file_name}"
        upload_to_gcs(local_path, gcs_path)
        logging.info(f"GCS: {gcs_path}")
        
        # Clean up local file
        os.remove(local_path)
        logging.info(f"Successfully processed {file_name}")
        return True
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.warning(f"File not found (404): {url}")
            return False
        else:
            logging.error(f"Error downloading: {e}")
            return False
    except Exception as e:
        logging.error(f"Failed to process {file_name}: {str(e)}")
        return False    

def main():
    """Main ingestion pipeline."""
    
    # Validate required environment variables
    if not PROJECT_ID or not BUCKET_NAME:
        logging.error("Error: PROJECT_ID and BUCKET_NAME must be set")
        sys.exit(1)
    
    logging.info(f"\n📋 Configuration:")
    logging.info(f"  Project: {PROJECT_ID}")
    logging.info(f"  Bucket: {BUCKET_NAME}")
    logging.info(f"  Type: {DATASET_TYPE}")
    logging.info(f"  Years: {', '.join(YEARS)}")
    logging.info(f"  Batch: Months {START_MONTH} to {END_MONTH}\n")

    # Upload taxi zone lookup first
    logging.info("\n--- Uploading Taxi Zone Lookup ---")
    download_taxi_zone_lookup()

    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # Process each year
    for year in YEARS:
        year = year.strip()
        
        # Determine months to process
        if int(year) < current_year:
            # Past year: get all 12 months (ignore batch range)
            months = range(1, 13)
        elif int(year) == current_year:
            # Current year: only completed months, not current month
            months = range(START_MONTH, min(END_MONTH + 1, current_month))
        else:
            # Future year: skip
            logging.warning(f"Skipping future year {year}")
            continue
        
        # Process each month
        for month in months:
            logging.info(f"\n--- Processing {year}-{month:02d} ---")
            
            # Download and process
            success = download_and_process_file(year, month, DATASET_TYPE)
            
            if not success:
                logging.warning(f"Skipping {year}-{month:02d}")
                continue

    logging.info("\n✅ Pipeline completed!")


if __name__ == "__main__":
    main()