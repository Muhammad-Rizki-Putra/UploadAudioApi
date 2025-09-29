import os
import cloudinary
import cloudinary.uploader
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Import the new background task
from tasks import process_fingerprints

load_dotenv()

# --- Cloudinary Configuration ---
cloudinary.config(
  cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
  api_key = os.environ.get('CLOUDINARY_API_KEY'),
  api_secret = os.environ.get('CLOUDINARY_API_SECRET')
)

app = FastAPI(title="Song Fingerprinting Uploader API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.post("/upload-song/")
async def create_upload_file(audio_file: UploadFile = File(...)):
    """
    This endpoint is now VERY FAST.
    It uploads to Cloudinary, creates a background job, and returns immediately.
    """
    try:
        # 1. Upload the file to Cloudinary
        upload_result = cloudinary.uploader.upload(
            audio_file.file,
            resource_type="video"
        )
        file_url = upload_result.get('secure_url')
        if not file_url:
            raise HTTPException(status_code=500, detail="Failed to upload file to Cloudinary.")

        # 2. Create the background job with the Cloudinary URL
        process_fingerprints.delay(file_url, audio_file.filename)

        # 3. Immediately return a success response to the user
        return JSONResponse(
            status_code=202, # 202 Accepted
            content={"status": "queued", "message": f"'{audio_file.filename}' has been accepted and is being processed in the background."}
        )
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"message": "Welcome to the Song Fingerprinting API."}