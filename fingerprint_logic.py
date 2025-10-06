import os
import glob
import librosa
import numpy as np
import datetime # NEW: To get the current date
from scipy.ndimage import maximum_filter
import argparse
import psycopg2             # NEW: Use psycopg2 for PostgreSQL
import uuid                 # NEW: To generate unique varchar IDs for songs
from dotenv import load_dotenv # NEW: To load secrets from .env file
import uuid
from datetime import datetime
from psycopg2.extras import execute_values


# --- Database Setup ---
# NEW: Load environment variables from your .env file
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def connect_to_db():
    """Establishes a connection to the Supabase PostgreSQL database."""
    try:
        conn = psycopg2.connect(DB_URL)
        print("Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"🔥 Could not connect to the database: {e}")
        print("Please check your DATABASE_URL in the .env file.")
        return None

def fingerprint_song(file_path):
    """
    Generates a landmark-based fingerprint for a single audio file.
    
    This function is mostly the same as your original, but it returns a
    list of (hash, timestamp) tuples instead of a dictionary, which is
    easier to process for database insertion.
    
    Args:
        file_path (str): Path to the audio file.
        
    Returns:
        list: A list of tuples, where each tuple is (hash, anchor_time).
              Returns an empty list if an error occurs.
    """
    try:
        TARGET_SR = 11025
        # loading song  
        y, sr = librosa.load(file_path, sr=TARGET_SR)

        # fourier transformation
        D = librosa.stft(y)
        S_db = librosa.amplitude_to_db(np.abs(D), ref=np.max)

        neighborhood_size = 15
        local_max = maximum_filter(S_db, footprint=np.ones((neighborhood_size, neighborhood_size)), mode='constant')
        detected_peaks = (S_db == local_max)
        amplitude_threshold = -50.0
        peaks = np.where((detected_peaks) & (S_db > amplitude_threshold))
        
        if not peaks[0].any():
            return []

        n_fft = (D.shape[0] - 1) * 2
        peak_freqs_at_peaks = librosa.fft_frequencies(sr=sr, n_fft=n_fft)[peaks[0]]
        peak_times = librosa.frames_to_time(frames=peaks[1], sr=sr, n_fft=n_fft)
        peaks_list = list(zip(peak_times, peak_freqs_at_peaks))
        sorted_peaks = sorted(peaks_list, key=lambda p: p[0])

        fingerprints = []
        TARGET_ZONE_START_TIME = 0.1
        TARGET_ZONE_TIME_DURATION = 0.8
        TARGET_ZONE_FREQ_WIDTH = 200

        for i, anchor_peak in enumerate(sorted_peaks):
            anchor_time, anchor_freq = anchor_peak
            t_min = anchor_time + TARGET_ZONE_START_TIME
            t_max = t_min + TARGET_ZONE_TIME_DURATION
            f_min = anchor_freq - TARGET_ZONE_FREQ_WIDTH
            f_max = anchor_freq + TARGET_ZONE_FREQ_WIDTH
            
            for j in range(i + 1, len(sorted_peaks)):
                target_peak = sorted_peaks[j]
                target_time, target_freq = target_peak
                if target_time > t_max:
                    break
                if t_min <= target_time <= t_max and f_min <= target_freq <= f_max:
                    time_delta = target_time - anchor_time
                    h = hash((anchor_freq, target_freq, time_delta))
                    fingerprints.append((h, anchor_time))
        print(f"Generated {len(fingerprints)} fingerprints for {os.path.basename(file_path)}")
        return fingerprints

    except Exception as e:
        print(f"Could not process {file_path}. Error: {e}")
        return []


def generate_song_id():
    """Generate structured song ID: SONG_YYYYMMDD_SHORTUID"""
    date_part = datetime.now().strftime("%Y%m%d")
    uuid_part = str(uuid.uuid4())[:8].upper()
    return f"SONG_{date_part}_{uuid_part}"

def insert_song_and_fingerprints(conn, file_path, batch_size=5000):
    """Processes a single song and adds its fingerprints to the database in batches."""
    try:
        song_name = os.path.basename(file_path).replace('.mp3', '')
        cursor = conn.cursor()

        # Check if song already exists
        cursor.execute("SELECT szsongid FROM cala_mdm_songs WHERE szsongtitle = %s", (song_name,))
        result = cursor.fetchone()

        if result:
            print(f"'{song_name}' already exists in the database. Skipping.")
            cursor.close()
            return result[0]
        
        print(f"Processing: {song_name}...")
        song_id = generate_song_id()
        
        # Insert song record
        insert_query = """
        INSERT INTO cala_mdm_songs (
            szsongid, 
            szsongtitle, 
            szcomposerid, 
            szperformerid, 
            bactive, 
            szcreatedby, 
            dtmcreated, 
            dtmupdated
        ) VALUES (
            %s, %s, %s, %s, %s, %s, NOW(), NOW()
        )
        """
        cursor.execute(insert_query, (song_id, song_name, "UNKNOWN", "UNKNOWN", True, "admin"))
        conn.commit()  # Commit song insertion first
        print(f"✅ Song '{song_name}' inserted with ID: {song_id}")

        # Generate fingerprints
        fingerprints = fingerprint_song(file_path)

        if not fingerprints:
            print(f"No fingerprints generated for {song_name}. Nothing to add.")
            cursor.close()
            return song_id
        
        print(f"Inserting {len(fingerprints)} fingerprints in batches of {batch_size}...")
        
        # Prepare fingerprint data
        fingerprint_tuples = [(song_id, offset_time, hash_value) for hash_value, offset_time in fingerprints]
        
        # SOLUTION 1: Use COPY FROM for maximum performance
        try:
            # Create a temporary file with the data
            import tempfile
            import csv
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='') as temp_file:
                writer = csv.writer(temp_file)
                for song_id_val, offset_time, hash_value in fingerprint_tuples:
                    writer.writerow([song_id_val, offset_time, hash_value])
                temp_filename = temp_file.name
            
            # Use COPY FROM for bulk insert
            with open(temp_filename, 'r') as f:
                cursor.copy_from(
                    f, 
                    'CALA_MDM_FINGERPRINTS', 
                    columns=('szSongID', 'offSetTime', 'intHash'),
                    sep=','
                )
            
            # Clean up temporary file
            os.unlink(temp_filename)
            
        except Exception as copy_error:
            print(f"COPY FROM failed ({copy_error}), falling back to batch insert...")
            
            # SOLUTION 2: Batch insert fallback
            total_batches = (len(fingerprint_tuples) + batch_size - 1) // batch_size
            
            fingerprint_query = """
            INSERT INTO CALA_MDM_FINGERPRINTS (
                "szSongID",
                "offSetTime",
                "intHash"
            ) VALUES (%s, %s, %s)
            """
            
            for i in range(0, len(fingerprint_tuples), batch_size):
                batch = fingerprint_tuples[i:i + batch_size]
                current_batch = (i // batch_size) + 1
                
                print(f"  Processing batch {current_batch}/{total_batches} ({len(batch)} records)...")
                
                cursor.executemany(fingerprint_query, batch)
                conn.commit()  # Commit each batch
                
                # Optional: Add a small delay to prevent overwhelming the database
                # time.sleep(0.1)
        
        conn.commit()  # Final commit
        print(f"✅ All {len(fingerprint_tuples)} fingerprints inserted successfully!")
        
        cursor.close()
        return song_id

    except psycopg2.Error as e:
        print(f"❌ Error inserting song and fingerprints: {e}")
        conn.rollback()
        if 'cursor' in locals():
            cursor.close()
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        conn.rollback()
        if 'cursor' in locals():
            cursor.close()
        return None


# Alternative: Ultra-fast bulk insert using execute_values
def insert_song_and_fingerprints_fast(conn, file_path, original_filename, batch_size=5000):
    """Ultra-fast version using execute_values for bulk inserts."""
    try:
        
        song_name = os.path.splitext(original_filename)[0]
        cursor = conn.cursor()

        # Check if song already exists
        cursor.execute("SELECT szsongid FROM cala_mdm_songs WHERE szsongtitle = %s", (song_name,))
        result = cursor.fetchone()

        if result:
            print(f"'{song_name}' already exists in the database. Skipping.")
            cursor.close()
            return result[0]
        
        print(f"Processing: {song_name}...")
        song_id = generate_song_id()
        
        # Insert song record
        insert_query = """
        INSERT INTO cala_mdm_songs (
            szsongid, szsongtitle, szcomposerid, szperformerid, 
            bactive, szcreatedby, dtmcreated, dtmupdated
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        """
        cursor.execute(insert_query, (song_id, song_name, "UNKNOWN", "UNKNOWN", True, "admin"))
        conn.commit()
        print(f"✅ Song '{song_name}' inserted with ID: {song_id}")

        # Generate fingerprints
        fingerprints = fingerprint_song(file_path)
        if not fingerprints:
            print(f"No fingerprints generated for {song_name}.")
            cursor.close()
            return song_id
        
        print(f"Inserting {len(fingerprints)} fingerprints using execute_values...")
        
        # Prepare data for execute_values
        fingerprint_tuples = [(song_id, offset_time, hash_value) for hash_value, offset_time in fingerprints]
        
        # Use execute_values for ultra-fast bulk insert
        execute_values(
            cursor,
            """INSERT INTO "CALA_MDM_FINGERPRINTS" ("szSongID", "offSetTime", "intHash") VALUES %s""", 
            fingerprint_tuples,
            template=None,
            page_size=batch_size
        )
        
        conn.commit()
        print(f"✅ All {len(fingerprint_tuples)} fingerprints inserted successfully!")
        
        cursor.close()
        return song_id

    except psycopg2.Error as e:
        print(f"❌ Error inserting song and fingerprints: {e}")
        conn.rollback()
        if 'cursor' in locals():
            cursor.close()
        return None
    
def process_multiple_mp3_files(conn, folder_path):
    """Process multiple MP3 files in a folder."""
    mp3_files = glob.glob(os.path.join(folder_path, "*.mp3"))
    
    if not mp3_files:
        print("No MP3 files found in the specified folder.")
        return
    
    print(f"Found {len(mp3_files)} MP3 files to process...")
    
    for file_path in mp3_files:
        insert_song_and_fingerprints(conn, file_path)

# Main execution
if __name__ == "__main__":
    # Connect to database
    conn = connect_to_db()
    
    if conn:
        try:
            # Example: Process a single file
            # insert_song_and_fingerprints(conn, "D:\\Berkas_Rizki\\Semester_7\\Magang\\songs\\Hindia\\Hindia - Rumah Ke Rumah.mp3")

            # Example: Process all MP3 files in a folder
            process_multiple_mp3_files(conn, "D:\\Berkas_Rizki\\Semester_7\\Magang\\songs\\PayungTeduh")

            print("Processing completed!")
            
        finally:
            conn.close()
            print("Database connection closed.")
    else:
        print("Failed to connect to database. Exiting.")
