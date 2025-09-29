import os
import tempfile
import shutil
import requests # We'll use this to download the file from Cloudinary
import cloudinary
import cloudinary.uploader
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Import the functions from your original script
from fingerprint_logic import connect_to_db, insert_song_and_fingerprints_fast

# Load environment variables from .env file for local testing
load_dotenv()

# --- Cloudinary Configuration ---
cloudinary.config(
  cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
  api_key = os.environ.get('CLOUDINARY_API_KEY'),
  api_secret = os.environ.get('CLOUDINARY_API_SECRET')
)

# Initialize the FastAPI app
app = FastAPI(title="Song Fingerprinting Uploader API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def download_file_from_url(url: str) -> str:
    """Downloads a file from a URL and saves it to a temporary path."""
    response = requests.get(url, stream=True)
    response.raise_for_status() # Raise an exception for bad status codes

    # Create a temporary file to store the downloaded content
    temp_file_descriptor, temp_path = tempfile.mkstemp(suffix=".mp3")
    
    with open(temp_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            
    os.close(temp_file_descriptor)
    return temp_path


@app.post("/upload-song/")
async def create_upload_file(audio_file: UploadFile = File(...)):
    """
    Receives an audio file, uploads it to Cloudinary, downloads it back,
    processes it, and adds its fingerprints to the database.
    """
    temp_local_path = None
    conn = None

    try:
        # 1. Upload the file to Cloudinary for persistent storage
        print(f"Uploading '{audio_file.filename}' to Cloudinary...")
        upload_result = cloudinary.uploader.upload(
            audio_file.file,
            resource_type="video" # Use 'video' or 'raw' for audio files
        )
        file_url = upload_result.get('secure_url')
        if not file_url:
            raise HTTPException(status_code=500, detail="Failed to upload file to Cloudinary.")
        
        print(f"File uploaded to: {file_url}")

        # 2. Download the file from Cloudinary to a temporary local path for processing
        print("Downloading file from Cloudinary for local processing...")
        temp_local_path = download_file_from_url(file_url)

        # 3. Connect to the database
        conn = connect_to_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed.")

        # 4. Use the downloaded file path with your existing fingerprinting logic
        print(f"Processing fingerprints for '{temp_local_path}'...")
        song_id = insert_song_and_fingerprints_fast(conn, temp_local_path)

        if song_id:
            # Check if the song was skipped because it already exists
            if "already exists" in str(song_id):
                 return {"status": "skipped", "message": f"Song '{audio_file.filename}' already exists."}
            return {"status": "success", "message": f"Song '{audio_file.filename}' processed successfully.", "song_id": song_id}
        else:
            raise HTTPException(status_code=500, detail="Failed to process fingerprints.")

    except Exception as e:
        # If anything goes wrong, return an error
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # 5. Clean up everything
        if conn:
            conn.close()
        if temp_local_path and os.path.exists(temp_local_path):
            os.unlink(temp_local_path)
            print(f"Cleaned up temporary file: {temp_local_path}")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Song Fingerprinting API."}