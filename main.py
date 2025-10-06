import os
import cloudinary
import cloudinary.uploader
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pydantic import BaseModel

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

class UploadURL(BaseModel):
    file_url: str
    original_filename: str

@app.post("/upload-song/")
async def create_upload_file(audio_file: UploadFile = File(...)):
    """
    This endpoint is now VERY FAST.
    It uploads to Cloudinary, creates a background job, and returns immediately.
    """
    try:
      process_fingerprints.delay(data.file_url, data.original_filename)
      
      return JSONResponse(
            status_code=202, # 202 Accepted
            content={"status": "queued", "message": f"'{data.original_filename}' has been accepted and is being processed in the background."}
        )

    except Exception as e:
       return HTTPException(status_code=500, detail=f"Failed to queue job: {str(e)}")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Song Fingerprinting API."}
