from flask import Flask, request, jsonify
import yt_dlp
import os
from google.cloud import storage
import uuid
import logging

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# Tên Bucket GCS
BUCKET_NAME = os.environ.get("GCS_BUCKET", "")

def upload_to_gcs(local_path, bucket_name, dest_blob):
    """Upload file lên GCS và trả về link public"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob)
    blob.upload_from_filename(local_path)
    return f"https://storage.googleapis.com/{bucket_name}/{dest_blob}"

@app.route("/")
def home():
    return "Audio Downloader (Sync Mode) is Ready!"

@app.route("/download-audio", methods=["POST"])
def download_audio():
    # 1. Nhận dữ liệu
    data = request.get_json(silent=True)
    if not data or "url" not in data:
        return jsonify({"status": "error", "message": "Thiếu tham số 'url'"}), 400

    url = data["url"]
    job_id = str(uuid.uuid4())
    
    # Cấu hình yt-dlp
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'/tmp/{job_id}.%(ext)s',
        'cookiefile': '/app/cookies.txt', # Giữ nguyên nếu bạn có dùng cookies
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'quiet': True,
        'no_warnings': True,
    }

    final_filename = f"/tmp/{job_id}.mp3"

    try:
        app.logger.info(f"Đang xử lý: {url}")
        
        # 2. Tải và Convert (Code sẽ dừng ở đây đợi xong mới chạy tiếp)
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get("title", "audio")

        # 3. Upload lên GCS
        if os.path.exists(final_filename):
            file_size = os.path.getsize(final_filename)
            
            if not BUCKET_NAME:
                return jsonify({"status": "error", "message": "Chưa cấu hình GCS_BUCKET"}), 500
            
            dest_blob = f"audio/{job_id}.mp3"
            gcs_url = upload_to_gcs(final_filename, BUCKET_NAME, dest_blob)
            
            # 4. Trả về kết quả NGAY LẬP TỨC
            return jsonify({
                "status": "success",
                "title": title,
                "url": url,
                "download_url": gcs_url,
                "file_size": file_size
            })
        else:
            return jsonify({"status": "error", "message": "Lỗi: Không tìm thấy file sau khi tải"}), 500

    except Exception as e:
        app.logger.error(f"Lỗi: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        # Dọn dẹp file rác
        if os.path.exists(final_filename):
            os.remove(final_filename)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
