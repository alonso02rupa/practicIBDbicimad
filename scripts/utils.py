# File: scripts/utils.py
from minio import Minio
import pandas as pd
import io
import trino
import os
import json
import datetime
import hashlib

def get_minio_client():
    """Create and return a MinIO client."""
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def get_trino_connection():
    """Create and return a Trino connection."""
    return trino.dbapi.connect(
        host="trino",
        port=8080,
        user="trino",
        catalog="minio",
        schema="default",
    )

def upload_file_to_minio(file_path, bucket_name, object_name=None):
    """Upload a file to MinIO."""
    if object_name is None:
        object_name = os.path.basename(file_path)

    client = get_minio_client()

    # Make sure the bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Upload the file
    client.fput_object(bucket_name, object_name, file_path)
    print(f"File {file_path} uploaded to {bucket_name}/{object_name}")

    # Store metadata in govern-zone-metadata
    store_file_metadata(bucket_name, object_name, file_path)

def download_file_from_minio(bucket_name, object_name, file_path=None):
    """Download a file from MinIO."""
    if file_path is None:
        file_path = object_name

    client = get_minio_client()
    client.fget_object(bucket_name, object_name, file_path)
    print(f"File {bucket_name}/{object_name} downloaded to {file_path}")

def upload_dataframe_to_minio(df, bucket_name, object_name, format='csv', metadata=None):
    """Upload a pandas DataFrame to MinIO with metadata."""
    client = get_minio_client()

    # Make sure the bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Convert DataFrame to bytes in the specified format
    if format.lower() == 'csv':
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        content_type = 'text/csv'
    elif format.lower() == 'parquet':
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        content_type = 'application/octet-stream'
    else:
        raise ValueError(f"Unsupported format: {format}")

    # Upload the data
    client.put_object(
        bucket_name, object_name, buffer,
        length=buffer.getbuffer().nbytes,
        content_type=content_type
    )

    print(f"DataFrame uploaded to {bucket_name}/{object_name}")

    # Store metadata
    if metadata is None:
        metadata = {}

    # Add basic metadata
    metadata.update({
        'uploaded_at': datetime.datetime.now().isoformat(),
        'format': format,
        'rows': len(df),
        'columns': list(df.columns),
        'column_types': {col: str(df[col].dtype) for col in df.columns}
    })

    # Store metadata in govern-zone-metadata
    store_object_metadata(bucket_name, object_name, metadata)

def download_dataframe_from_minio(bucket_name, object_name, format='csv'):
    """Download a file from MinIO into a pandas DataFrame."""
    client = get_minio_client()

    # Get the object
    response = client.get_object(bucket_name, object_name)

    # Convert to DataFrame based on format
    if format.lower() == 'csv':
        return pd.read_csv(response)
    elif format.lower() == 'parquet':
        return pd.read_parquet(io.BytesIO(response.read()))
    else:
        raise ValueError(f"Unsupported format: {format}")

def execute_trino_query(query):
    """Execute a query in Trino and return the results as a DataFrame."""
    conn = get_trino_connection()
    cursor = conn.cursor()
    cursor.execute(query)

    # Get column names
    if cursor.description:
        columns = [desc[0] for desc in cursor.description]
        # Fetch all results
        data = cursor.fetchall()
        # Create DataFrame
        return pd.DataFrame(data, columns=columns)
    else:
        return pd.DataFrame()

def store_file_metadata(bucket_name, object_name, file_path):
    """Store file metadata in the govern-zone-metadata bucket."""
    client = get_minio_client()

    # Calculate file hash for data lineage
    file_hash = calculate_file_hash(file_path)

    # Prepare metadata
    metadata = {
        'source_bucket': bucket_name,
        'object_name': object_name,
        'original_file_path': file_path,
        'uploaded_at': datetime.datetime.now().isoformat(),
        'file_hash': file_hash,
        'file_size': os.path.getsize(file_path)
    }

    # Store metadata
    metadata_json = json.dumps(metadata)
    metadata_buffer = io.BytesIO(metadata_json.encode('utf-8'))

    metadata_object_name = f"metadata/{bucket_name}/{object_name.replace('/', '_')}.json"

    if not client.bucket_exists('govern-zone-metadata'):
        client.make_bucket('govern-zone-metadata')

    client.put_object(
        'govern-zone-metadata',
        metadata_object_name,
        metadata_buffer,
        length=len(metadata_json),
        content_type='application/json'
    )

    print(f"Metadata stored in govern-zone-metadata/{metadata_object_name}")

def store_object_metadata(bucket_name, object_name, metadata):
    """Store object metadata in the govern-zone-metadata bucket."""
    client = get_minio_client()

    # Add source information
    metadata.update({
        'source_bucket': bucket_name,
        'object_name': object_name,
    })

    # Store metadata
    metadata_json = json.dumps(metadata)
    metadata_buffer = io.BytesIO(metadata_json.encode('utf-8'))

    metadata_object_name = f"metadata/{bucket_name}/{object_name.replace('/', '_')}.json"

    if not client.bucket_exists('govern-zone-metadata'):
        client.make_bucket('govern-zone-metadata')

    client.put_object(
        'govern-zone-metadata',
        metadata_object_name,
        metadata_buffer,
        length=len(metadata_json),
        content_type='application/json'
    )

    print(f"Metadata stored in govern-zone-metadata/{metadata_object_name}")

def calculate_file_hash(file_path):
    """Calculate SHA-256 hash of a file for data lineage tracking."""
    sha256_hash = hashlib.sha256()

    with open(file_path, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()

def log_data_transformation(source_bucket, source_object, target_bucket, target_object, transformation_description):
    """Log data transformation details for data lineage and governance."""
    client = get_minio_client()

    # Prepare lineage metadata
    lineage = {
        'timestamp': datetime.datetime.now().isoformat(),
        'source': {
            'bucket': source_bucket,
            'object': source_object
        },
        'target': {
            'bucket': target_bucket,
            'object': target_object
        },
        'transformation': transformation_description
    }

    # Store lineage information
    lineage_json = json.dumps(lineage)
    lineage_buffer = io.BytesIO(lineage_json.encode('utf-8'))

    lineage_object_name = f"lineage/{source_bucket}_{source_object.replace('/', '_')}_to_{target_bucket}_{target_object.replace('/', '_')}.json"

    if not client.bucket_exists('govern-zone-metadata'):
        client.make_bucket('govern-zone-metadata')

    client.put_object(
        'govern-zone-metadata',
        lineage_object_name,
        lineage_buffer,
        length=len(lineage_json),
        content_type='application/json'
    )

    print(f"Transformation lineage stored in govern-zone-metadata/{lineage_object_name}")

def convert_to_serializable(obj):
    """Convert object to JSON serializable type."""
    import numpy as np
    if isinstance(obj, np.bool_):  # Use only np.bool_
        return bool(obj)
    elif isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    return obj

def validate_data_quality(df, dataset_name, rules=None):
    """Perform basic data quality checks and log results to govern-zone."""
    if rules is None:
        # Default rules: check for nulls and duplicates
        rules = {
            'no_nulls': [],  # Columns that shouldn't have nulls
            'unique': []     # Columns that should be unique
        }

    quality_results = {
        'dataset': dataset_name,
        'timestamp': datetime.datetime.now().isoformat(),
        'row_count': len(df),
        'checks': []
    }

    # Check for nulls
    for col in rules.get('no_nulls', []):
        if col in df.columns:
            null_count = df[col].isnull().sum()
            quality_results['checks'].append({
                'check': 'no_nulls',
                'column': col,
                'passed': null_count == 0,
                'details': f"{null_count} null values found"
            })

    # Check for uniqueness
    for col in rules.get('unique', []):
        if col in df.columns:
            unique_count = df[col].nunique()
            is_unique = unique_count == len(df)
            quality_results['checks'].append({
                'check': 'unique',
                'column': col,
                'passed': is_unique,
                'details': f"{len(df) - unique_count} duplicate values found"
            })

    # Store quality check results
    client = get_minio_client()

    # Convert to serializable format before JSON dump
    def make_serializable(data):
        if isinstance(data, dict):
            return {k: make_serializable(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [make_serializable(i) for i in data]
        else:
            return convert_to_serializable(data)

    serializable_results = make_serializable(quality_results)
    quality_json = json.dumps(serializable_results)

    quality_buffer = io.BytesIO(quality_json.encode('utf-8'))

    quality_object_name = f"quality/{dataset_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    if not client.bucket_exists('govern-zone-metadata'):
        client.make_bucket('govern-zone-metadata')

    client.put_object(
        'govern-zone-metadata',
        quality_object_name,
        quality_buffer,
        length=len(quality_json),
        content_type='application/json'
    )

    print(f"Data quality results stored in govern-zone-metadata/{quality_object_name}")
    return quality_results