from datetime import datetime, timezone
import os
from typing import Any, Dict, Optional, List
import aioboto3
import zstandard as zstd
import pydantic
import logging
import asyncio
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)

class TigrisBucketResult(pydantic.BaseModel):
    key_name: str
    file_name: str
    file_size: float = pydantic.Field(
        ge=0
    )  # ensures file size is non-negative in kilobytes
    file_compressed_size: float = pydantic.Field(ge=0)
    created_at: datetime = pydantic.Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = pydantic.Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_str(self) -> str:
        """Return a formatted string representation"""
        return (
            f"File: {self.file_name}\n"
            f"Key: {self.key_name}\n"
            f"Size: {self.file_size} KB\n"
            f"Created: {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def to_json(self) -> str:
        """Return JSON string"""
        return self.model_dump_json()

    def to_dict(self) -> Dict[str, Any]:
        """Return dictionary representation"""
        return self.model_dump()

# Tigris Buckets configuration
TIGRIS_BUCKET_NAME = "crawlagent.bucket.a"
TIGRIS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
TIGRIS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TIGRIS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL_S3")

# Create a reusable session
session = aioboto3.Session()
_client = None

# Get or create S3 client with proper context management
async def get_s3_client():
    """Get S3 client as a context manager."""
    global _client
    if _client is None:
        _client = await session.client(
            "s3",
            endpoint_url=TIGRIS_ENDPOINT_URL,
            aws_access_key_id=TIGRIS_ACCESS_KEY,
            aws_secret_access_key=TIGRIS_SECRET_KEY,
        ).__aenter__()
    return _client

# Use this function to create a context manager
class S3ClientManager:
    async def __aenter__(self):
        self.client = await get_s3_client()
        return self.client
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Don't close the global client
        pass

async def folder_exists(folder_name: str) -> bool:
    """Check if a folder exists in Tigris Buckets."""
    try:
        async with S3ClientManager() as svc:
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME, 
                Prefix=f"{folder_name}/", 
                MaxKeys=1
            )
            return "Contents" in response
    except ClientError as e:
        logger.error(f"S3 error checking folder {folder_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking folder {folder_name}: {e}")
        return False

async def upload_compressed_file(
    markdown_str: str, folder_name: str, file_name: str
) -> Optional[TigrisBucketResult]:
    """Uploads a Markdown string compressed with zstd to Tigris Buckets."""
    
    file_size_kb = len(markdown_str.encode("utf-8")) / 1024
    key_name = f"{folder_name}/{file_name}"
    
    # Compress data
    try:
        compressor = zstd.ZstdCompressor(level=3)  # Level 3 balances speed and compression
        compressed_data = compressor.compress(markdown_str.encode("utf-8"))
        file_compressed_size = len(compressed_data) / 1024
        
        # Determine if we should use multipart upload (>5MB)
        use_multipart = len(compressed_data) > 5 * 1024 * 1024
        
        async with S3ClientManager() as svc:
            if use_multipart:
                # For large files, use multipart upload
                return await _upload_multipart(
                    svc, compressed_data, key_name, file_name, file_size_kb, file_compressed_size
                )
            else:
                # For smaller files, use simple put_object
                await svc.put_object(
                    Bucket=TIGRIS_BUCKET_NAME,
                    Key=key_name,
                    Body=compressed_data,
                    ContentType="text/markdown",
                )
                logger.info(f"Uploaded {file_name} to {key_name} ({file_size_kb:.2f} KB → {file_compressed_size:.2f} KB)")
                
                return TigrisBucketResult(
                    key_name=key_name,
                    file_name=file_name,
                    file_size=file_size_kb,
                    file_compressed_size=file_compressed_size,
                )
    except ClientError as e:
        logger.error(f"S3 upload error for {key_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Compression/upload error for {file_name}: {e}")
        return None

async def _upload_multipart(svc, data, key_name, file_name, file_size_kb, file_compressed_size):
    """Helper function for multipart uploads of large files."""
    try:
        # Create multipart upload
        mpu = await svc.create_multipart_upload(
            Bucket=TIGRIS_BUCKET_NAME,
            Key=key_name,
            ContentType="text/markdown"
        )
        
        # Split data into chunks (5MB per chunk)
        chunk_size = 5 * 1024 * 1024
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        
        # Upload parts
        parts = []
        for i, chunk in enumerate(chunks):
            part_number = i + 1
            response = await svc.upload_part(
                Bucket=TIGRIS_BUCKET_NAME,
                Key=key_name,
                PartNumber=part_number,
                UploadId=mpu["UploadId"],
                Body=chunk
            )
            parts.append({
                "PartNumber": part_number,
                "ETag": response["ETag"]
            })
        
        # Complete multipart upload
        await svc.complete_multipart_upload(
            Bucket=TIGRIS_BUCKET_NAME,
            Key=key_name,
            UploadId=mpu["UploadId"],
            MultipartUpload={"Parts": parts}
        )
        
        logger.info(f"Multipart uploaded {file_name} to {key_name} ({file_size_kb:.2f} KB → {file_compressed_size:.2f} KB)")
        
        return TigrisBucketResult(
            key_name=key_name,
            file_name=file_name,
            file_size=file_size_kb,
            file_compressed_size=file_compressed_size,
        )
    except Exception as e:
        logger.error(f"Multipart upload error: {e}")
        # Try to abort the multipart upload to avoid orphaned uploads
        try:
            await svc.abort_multipart_upload(
                Bucket=TIGRIS_BUCKET_NAME,
                Key=key_name,
                UploadId=mpu["UploadId"]
            )
        except Exception as abort_error:
            logger.error(f"Failed to abort multipart upload: {abort_error}")
        return None

async def download_file_decompressed(folder_name: str, file_name: str) -> Optional[str]:
    """Downloads and decompresses a file from Tigris Buckets."""
    key_name = f"{folder_name}/{file_name}"
    
    # Implement exponential backoff retry for downloads
    max_retries = 3
    retry_delay = 1  # Start with 1 second delay
    
    for attempt in range(max_retries):
        try:
            async with S3ClientManager() as svc:
                response = await svc.get_object(
                    Bucket=TIGRIS_BUCKET_NAME, 
                    Key=key_name
                )
                compressed_data = await response["Body"].read()
                
                # Decompress the data
                decompressor = zstd.ZstdDecompressor()
                decompressed_data = decompressor.decompress(compressed_data)
                str_file = decompressed_data.decode("utf-8")
                
                logger.info(f"Downloaded and decompressed {file_name} from {key_name}")
                return str_file
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"File not found: {key_name}")
                return None
            logger.warning(f"S3 error on attempt {attempt+1}/{max_retries}: {e}")
        except Exception as e:
            logger.warning(f"Download error on attempt {attempt+1}/{max_retries}: {e}")
        
        # Only sleep if we're going to retry
        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
    
    logger.error(f"Download failed after {max_retries} attempts: {key_name}")
    return None

async def list_files(folder_name: str) -> List[Dict[str, Any]]:
    """List all files in a folder."""
    try:
        async with S3ClientManager() as svc:
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME,
                Prefix=f"{folder_name}/"
            )
            
            if "Contents" not in response:
                return []
                
            return [
                {
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                    "file_name": obj["Key"].split("/")[-1]
                }
                for obj in response["Contents"]
            ]
    except Exception as e:
        logger.error(f"Error listing files in {folder_name}: {e}")
        return []

async def get_presigned_url(folder_name: str, file_name: str, expiration: int = 3600) -> Optional[str]:
    """
    Generate a presigned URL for direct download of the compressed file.
    
    Args:
        folder_name: The folder/prefix containing the file
        file_name: The file name to download
        expiration: URL expiration time in seconds (default 1 hour)
        
    Returns:
        Presigned URL string or None if error
    """
    key_name = f"{folder_name}/{file_name}"
    
    try:
        async with S3ClientManager() as svc:
            # Create the presigned URL
            url = await svc.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': TIGRIS_BUCKET_NAME,
                    'Key': key_name
                },
                ExpiresIn=expiration
            )
            
            logger.info(f"Generated presigned URL for {key_name}, expires in {expiration} seconds")
            return url
            
    except Exception as e:
        logger.error(f"Error generating presigned URL for {key_name}: {e}")
        return None

async def download_and_decompress_stream(folder_name: str, file_name: str):
    """
    Downloads and decompresses a file, returning it as a streaming response.
    This function should be used with FastAPI's StreamingResponse.
    
    Args:
        folder_name: The folder/prefix containing the file
        file_name: The file name to download
        
    Returns:
        An async generator yielding decompressed content
    """
    key_name = f"{folder_name}/{file_name}"
    
    async def content_stream():
        try:
            async with S3ClientManager() as svc:
                response = await svc.get_object(
                    Bucket=TIGRIS_BUCKET_NAME, 
                    Key=key_name
                )
                
                # Use zstd streaming decompression for memory efficiency
                decompressor = zstd.ZstdDecompressor()
                compressed_stream = response["Body"]
                
                # Read and decompress in chunks
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = await compressed_stream.read(chunk_size)
                    if not chunk:
                        break
                        
                    # Decompress chunk and yield
                    yield decompressor.decompress(chunk)
                
                logger.info(f"Streamed and decompressed {file_name} from {key_name}")
                
        except ClientError as e:
            logger.error(f"S3 error streaming file {key_name}: {e}")
            yield f"Error: {str(e)}".encode('utf-8')
        except Exception as e:
            logger.error(f"Error streaming and decompressing {key_name}: {e}")
            yield f"Error: {str(e)}".encode('utf-8')
    
    return content_stream()
