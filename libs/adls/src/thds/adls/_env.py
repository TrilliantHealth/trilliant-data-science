"""This is where fine-tuning environment variables are defined."""
import os

# These defaults were tested to perform well (~200 MB/sec) on a 2 core
# machine on Kubernetes.  Larger numbers did not do any better, but
# these numbers did roughly 4x as well as the defaults, which are
# concurrency=1 and chunk_get_size=32 MB.
#
# As always, your mileage may vary.
#
# For more info, see docs at
# https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-download-python#specify-data-transfer-options-on-download
#
# Also see
# azure.storage.filedatalake._shared.base_client.create_configuration
# for actual details...
#
DOWNLOAD_FILE_MAX_CONCURRENCY = int(os.getenv("THDS_ADLS_DOWNLOAD_FILE_MAX_CONCURRENCY") or 4)
MAX_CHUNK_GET_SIZE = int(os.getenv("THDS_ADLS_MAX_CHUNK_GET_SIZE") or 2**20 * 64)  # 64MB
MAX_SINGLE_GET_SIZE = max(
    int(os.getenv("THDS_ADLS_MAX_SINGLE_GET_SIZE") or 2**20 * 64),  # 64MB
    MAX_CHUNK_GET_SIZE,
)

# these are for upload
# these achieved 380 MB/sec on a 2 core machine on Kubernetes
MAX_BLOCK_SIZE = int(os.getenv("THDS_ADLS_MAX_BLOCK_PUT_SIZE") or 2**20 * 64)  # 64MB
UPLOAD_FILE_MAX_CONCURRENCY = int(os.getenv("THDS_ADLS_UPLOAD_FILE_MAX_CONCURRENCY") or 10)
UPLOAD_CHUNK_SIZE = int(os.getenv("THDS_ADLS_UPLOAD_CHUNK_SIZE") or 2**20 * 100)  # 100MB

CONNECTION_TIMEOUT = 2000  # seconds
