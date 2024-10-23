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
def upload_file_to_s3(file, full_path):
    try:
        if file and file.filename:
            # The full path already contains folder structure
            filename = secure_filename(file.filename)
            key = full_path  # Use the full relative path sent from the frontend as the S3 key

            print(f"Uploading {key} to S3...")
            s3_client.upload_fileobj(file, S3_BUCKET, key)
            print(f"Uploaded {key} successfully!")
    except Exception as e:
        print(f"Failed to upload {file.filename}: {str(e)}")
        raise

@app.route('/upload', methods=['POST'])
def upload_file():
    print("call to /upload")
    files = request.files.getlist('files')  # Get all files including folder structure
    folder_name = request.form.get('folder_name')  # Get the optional folder name from the form

    if files:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for file in files:
                # Get the full path for each file (folder structure included)
                full_path = file.filename  # This includes the folder structure passed by webkitRelativePath
                if folder_name:
                    # Prepend the optional folder name
                    full_path = f"{folder_name}/{full_path}"

                # Submit each file for upload with its full path
                futures.append(executor.submit(upload_file_to_s3, file, full_path))

            concurrent.futures.wait(futures)

        return jsonify({'message': 'Files uploaded successfully!'}), 201

    return jsonify({'message': 'File upload failed.'}), 400

@app.route('/delete', methods=['DELETE'])
def delete_files():
    data = request.get_json()
    filenames = data.get('filenames', [])

    if not filenames:
        return jsonify({'message': 'No files specified for deletion.'}), 400

    try:
        # Use ThreadPoolExecutor to delete files concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(s3_client.delete_object, Bucket=S3_BUCKET, Key=filename) for filename in filenames]
            concurrent.futures.wait(futures)

        return jsonify({'message': 'Selected files deleted successfully.'}), 200
    except Exception as e:
        return jsonify({'message': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5544)
