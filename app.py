from flask import Flask, request, jsonify
import yt_dlp
import os
from google.cloud import storage
import threading
import uuid
import glob
import logging

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# Tên Bucket GCS lấy từ biến môi trường
BUCKET_NAME = os.environ.get("GCS_BUCKET", "")

def upload_to_gcs(local_path, bucket_name, dest_blob):
    """Upload file lên GCS và trả về link"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob)
    blob.upload_from_filename(local_path)
    return f"https://storage.googleapis.com/{bucket_name}/{dest_blob}"

def process_audio_job(job_id, url, callback_url):
    """Tải audio và upload"""
    # Cấu hình chỉ tải Audio và convert sang MP3
    ydl_opts = {
        'format': 'bestaudio/best',  # Chỉ tìm audio tốt nhất
        'outtmpl': '/tmp/%(id)s.%(ext)s',
        'cookiefile': '/app/cookies.txt', # Vẫn cần cho FB nếu video không public
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',      # Ép chuyển sang MP3 cho thông dụng
            'preferredquality': '192',
        }],
        'quiet': True,
        'no_warnings': True,
    }

    result = None
    final_filename = None

    try:
        app.logger.info(f"Job {job_id}: Bắt đầu tải audio {url}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            video_id = info.get("id")
            
            # Lưu ý: Vì dùng postprocessor convert sang mp3, 
            # tên file thực tế sẽ có đuôi .mp3 thay vì đuôi gốc (như .m4a, .webm)
            # Ta cần đoán đúng tên file kết quả:
            original_ext = info.get("ext")
            temp_path = ydl.prepare_filename(info)
            final_filename = temp_path.rsplit('.', 1)[0] + ".mp3"

        if os.path.exists(final_filename):
            file_size = os.path.getsize(final_filename)
            app.logger.info(f"Job {job_id}: Tải xong MP3, size={file_size}")

            if not BUCKET_NAME:
                result = {"status": "error", "message": "Chưa cấu hình GCS_BUCKET"}
            else:
                # Upload lên GCS
                dest_blob = f"audio/{os.path.basename(final_filename)}" # Gom vào thư mục audio/ cho gọn
                gcs_url = upload_to_gcs(final_filename, BUCKET_NAME, dest_blob)
                
                result = {
                    "status": "ok",
                    "job_id": job_id,
                    "title": info.get("title"),
                    "type": "audio",
                    "url": url,
                    "download_url": gcs_url
                }
        else:
            result = {"status": "error", "message": "Không tìm thấy file MP3 sau khi tải"}

    except Exception as e:
        app.logger.error(f"Job {job_id} Lỗi: {str(e)}")
        result = {"status": "error", "message": str(e)}

    finally:
        # Dọn dẹp file tạm
        if final_filename and os.path.exists(final_filename):
            os.remove(final_filename)

    # Gửi kết quả về n8n (hoặc nơi gọi)
    try:
        import requests
        requests.post(callback_url, json=result, timeout=10)
    except Exception as e:
        app.logger.warning(f"Callback lỗi: {e}")

@app.route("/download-audio", methods=["POST"])
def download_audio():
    data = request.get_json(silent=True)
    if not data or "url" not in data or "callback_url" not in data:
        return jsonify({"status": "error", "message": "Thiếu url hoặc callback_url"}), 400

    job_id = str(uuid.uuid4())
    url = data["url"]
    callback_url = data["callback_url"]

    # Chạy ngầm để không bị timeout request
    thread = threading.Thread(target=process_audio_job, args=(job_id, url, callback_url))
    thread.start()

    return jsonify({"status": "queued", "job_id": job_id})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
