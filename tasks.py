import os
import ssl
import tempfile
import requests
from celery import Celery

# Import your database and fingerprinting logic
from fingerprint_logic import connect_to_db, insert_song_and_fingerprints_fast

# --- Celery Configuration ---
# Heroku Redis requires special SSL settings
redis_url = os.environ.get('REDIS_URL')
if redis_url.startswith('rediss://'):
    celery_app = Celery('tasks', broker=redis_url, backend=redis_url, 
                        broker_use_ssl={'ssl_cert_reqs': ssl.CERT_NONE}, 
                        redis_backend_use_ssl={'ssl_cert_reqs': ssl.CERT_NONE})
else:
    celery_app = Celery('tasks', broker=redis_url, backend=redis_url)


def download_file_from_url(url: str, original_filename: str) -> str:
    """Downloads a file and saves it to a temp path with the correct extension."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Get the file extension from the original filename
    suffix = os.path.splitext(original_filename)[1]
    
    temp_file_descriptor, temp_path = tempfile.mkstemp(suffix=suffix)
    with open(temp_path, 'wb') as f:
        shutil.copyfileobj(response.raw, f)
            
    os.close(temp_file_descriptor)
    return temp_path

@celery_app.task
def process_fingerprints(file_url, original_filename):
    """
    This is the background job. It does all the slow work.
    """
    temp_local_path = None
    conn = None
    print(f"WORKER: Received job for {original_filename}")
    
    try:
        # 1. Download the file from Cloudinary to a temporary local path
        print("WORKER: Downloading file from Cloudinary...")
        temp_local_path = download_file_from_url(file_url, original_filename)
        
        # 2. Connect to the database
        conn = connect_to_db()
        if not conn:
            raise Exception("WORKER: Database connection failed.")
            
        # 3. Run the heavy fingerprinting and database insertion logic
        print(f"WORKER: Starting fingerprinting for {temp_local_path}...")
        song_id = insert_song_and_fingerprints_fast(conn, temp_local_path, original_filename)
        
        if song_id:
            print(f"WORKER: Successfully processed and inserted song. ID: {song_id}")
            return {"status": "success", "song_id": song_id}
        else:
            raise Exception("WORKER: Failed to process fingerprints.")

    except Exception as e:
        print(f"WORKER: Error processing {original_filename}. Reason: {e}")
        # Celery will automatically mark this task as failed
        raise e
    finally:
        # 4. Clean up
        if conn:
            conn.close()
        if temp_local_path and os.path.exists(temp_local_path):
            os.unlink(temp_local_path)
            print(f"WORKER: Cleaned up temporary file: {temp_local_path}")