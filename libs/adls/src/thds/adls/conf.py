"""This is where fine-tuning environment variables are defined."""
from thds.core import config

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
DOWNLOAD_FILE_MAX_CONCURRENCY = config.item("download_file_max_concurrency", 4, parse=int)
MAX_CHUNK_GET_SIZE = config.item("max_chunk_get_size", 2**20 * 64, parse=int)  # 64MB
MAX_SINGLE_GET_SIZE = config.item(
    "max_single_get_size", 2**20 * 64, parse=lambda i: max(MAX_CHUNK_GET_SIZE(), int(i))
)  # 64MB
MAX_SINGLE_PUT_SIZE = config.item(
    "max_single_put_size", 2**20 * 64, parse=lambda i: max(MAX_CHUNK_GET_SIZE(), int(i))
)  # 64MB

# these are for upload
# these achieved 380 MB/sec on a 2 core machine on Kubernetes
MAX_BLOCK_SIZE = config.item("max_block_put_size", 2**20 * 64, parse=int)  # 64 MB
UPLOAD_FILE_MAX_CONCURRENCY = config.item("upload_file_max_concurrency", 10, parse=int)
UPLOAD_CHUNK_SIZE = config.item("upload_chunk_size", 2**20 * 100, parse=int)  # 100 MB

CONNECTION_TIMEOUT = config.item("connection_timeout", 2000, parse=int)  # seconds
