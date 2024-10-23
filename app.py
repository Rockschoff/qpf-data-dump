import os
import boto3
from flask import Flask, request, jsonify, render_template
from werkzeug.utils import secure_filename
import concurrent.futures

# Initialize the Flask app
app = Flask(__name__)

# Configure AWS S3
S3_BUCKET = "niagara-qpf-data-dump"
s3_client = boto3.client('s3')

# Home route
@app.route('/')
def index():
    return render_template('index.html')

# Route to list all files in the S3 bucket
@app.route('/files', methods=['GET'])
def list_files():
    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET)
    files = [obj['Key'] for obj in objects.get('Contents', [])]
    return jsonify(files)

# Route to upload multiple files to the S3 bucket
def upload_file_to_s3(file):
    try:
        if file and file.filename:
            filename = secure_filename(file.filename)
            print(f"Uploading {filename} to S3...")
            s3_client.upload_fileobj(file, S3_BUCKET, filename)
            print(f"Uploaded {filename} successfully!")
    except Exception as e:
        print(f"Failed to upload {file.filename}: {str(e)}")
        raise

@app.route('/upload', methods=['POST'])
def upload_file():
    print("I AM HERE")
    files = request.files.getlist('files')  # Handle multiple files
    if files:
        print("Total number of files are", len(files))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Upload files concurrently
            futures = [executor.submit(upload_file_to_s3, file) for file in files]
            # Wait for all uploads to complete
            concurrent.futures.wait(futures)

        return jsonify({'message': 'Files uploaded successfully!'}), 201

    return jsonify({'message': 'File upload failed.'}), 400

# Route to delete a file from the S3 bucket
@app.route('/delete/<filename>', methods=['DELETE'])
def delete_file(filename):
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=filename)
        return jsonify({'message': f'{filename} deleted successfully.'}), 200
    except Exception as e:
        return jsonify({'message': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5544)
