import os
import time
import boto3
import pandas as pd
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- AWS Configuration ---
S3_BUCKET = os.environ.get("S3_BUCKET_NAME")
S3_REGION = os.environ.get("AWS_DEFAULT_REGION")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    region_name=S3_REGION,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# --- Helper: wait until file exists in S3 ---
def wait_for_file(key, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except Exception:
            time.sleep(2)
    return False

# --- Endpoint: process file ---
@app.route("/process", methods=["POST"])
def process_file():
    data = request.get_json()
    if not data or "s3_key" not in data:
        return jsonify({"error": "Missing 's3_key' in JSON body"}), 400

    s3_key = data["s3_key"]
    local_pdf = "/tmp/input.pdf"
    output_excel = "/tmp/output.xlsx"

    # 1️⃣ Wait for file to exist
    if not wait_for_file(s3_key):
        return jsonify({"error": "File not found in S3 after waiting"}), 404

    # 2️⃣ Download from S3
    s3_client.download_file(S3_BUCKET, s3_key, local_pdf)

    # 3️⃣ Simulate processing: generate sample Excel BOQ
    df = pd.DataFrame({
        "Item": ["Wall Finish", "Floor Finish", "Ceiling Paint"],
        "Quantity": [120, 200, 150],
        "Unit": ["m2", "m2", "m2"]
    })
    df.to_excel(output_excel, index=False)

    # 4️⃣ Upload Excel result to S3
    output_key = s3_key.replace("input.pdf", "output.xlsx")
    s3_client.upload_file(output_excel, S3_BUCKET, output_key)

    # 5️⃣ Generate presigned URL
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": output_key},
        ExpiresIn=3600
    )

    return jsonify({
        "status": "ok",
        "download_url": url,
        "message": "Excel BOQ generated successfully"
    })

@app.route("/", methods=["GET"])
def home():
    return jsonify({"service": "DraftQ Processor", "status": "running"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)