import os
import tempfile
import shutil
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Import the functions from your original script
from fingerprint_logic import connect_to_db, insert_song_and_fingerprints_fast

# Initialize the FastAPI app
app = FastAPI(title="Song Fingerprinting API")

# Configure CORS to allow requests from your cPanel frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For testing, allow all. For production, change to your cPanel domain.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload-song/")
async def create_upload_file(audio_file: UploadFile = File(...)):
    """
    Receives an audio file, saves it temporarily, processes it,
    and adds its fingerprints to the database.
    """
    # Your fingerprinting code needs a file path, not an in-memory file.
    # So, we save the uploaded file to a temporary location first.
    with tempfile.NamedTemporaryFile(delete=False, suffix=audio_file.filename) as temp_audio_file:
        shutil.copyfileobj(audio_file.file, temp_audio_file)
        temp_path = temp_audio_file.name

    conn = None
    try:
        conn = connect_to_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed.")

        # Use the file path with your existing logic
        song_id = insert_song_and_fingerprints_fast(conn, temp_path)

        if song_id:
            return {"status": "success", "message": f"Song '{audio_file.filename}' processed successfully.", "song_id": song_id}
        else:
            # Check if the song already existed
            if "already exists" in str(song_id): # This is a simple check, could be improved
                 return {"status": "skipped", "message": f"Song '{audio_file.filename}' already exists in the database."}
            raise HTTPException(status_code=500, detail="Failed to process fingerprints.")

    except Exception as e:
        # If anything goes wrong, return an error
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up the temporary file and database connection
        if conn:
            conn.close()
        if os.path.exists(temp_path):
            os.unlink(temp_path)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Song Fingerprinting API. Use the /upload-song/ endpoint to process files."}