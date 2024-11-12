# NOTE: This file contains duplicate content from Open-Sora-Serving/opensora_serving/utils/cos_utils.py
#       The following constants and code segments have been duplicated.
import asyncio
import logging
import os

from typing import Optional

import qcloud_cos
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosClientError, CosServiceError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

COS_CLIENT = None

COS_SECRET_ID = os.getenv("COS_SECRET_ID", "IKIDAOjw1znbHn9NSOAnHeqtlV6Xo18pimk4")
COS_SECRET_KEY = os.getenv("COS_SECRET_KEY", "IYm4FH9vUzPkvugcYVaYQAnHdr01ufHp")
COS_REGION = os.getenv("COS_REGION", "ap-beijing")
COS_BUCKET_NAME = os.getenv("COS_BUCKET_NAME", "video-ocean-data-1328930351")
COS_TOKEN = os.getenv("COS_TOKEN", None)
COS_SCHEME = os.getenv("COS_SCHEME", "https")

COS_OBJECT_PREFIX = os.getenv("COS_OBJECT_PREFIX", "test_live_portrait/")


def init_cos_client():
    global COS_CLIENT
    config = CosConfig(
        Region=COS_REGION, SecretId=COS_SECRET_ID, SecretKey=COS_SECRET_KEY, Token=COS_TOKEN, Scheme=COS_SCHEME
    )
    COS_CLIENT = CosS3Client(config)


def _download_file(object_key: str, dest_path: str, retries: int = 5) -> None:
    for i in range(0, max(1, retries)):
        try:
            response = COS_CLIENT.download_file(Bucket=COS_BUCKET_NAME, Key=object_key, DestFilePath=dest_path)
            break
        except CosClientError or CosServiceError as e:
            logger.error(e)


async def cos_download_file(object_path: str, save_path: str) -> bool:
    """
    Args:
        object_path: bucket-name/key
            E.g. video-ocean-data-XXXX/test_opensora/XXXX-uuid-YYYY-ZZZZ.png
        save_path: The local path to save the downloaded file.
    Returns:
        bool: True if the download is successful, False otherwise.
    """

    async def body(object_p: str, save_p: str) -> bool:

        download_success = False

        assert COS_CLIENT, "COS client should be init first."
        assert object_p and save_p, "The object path and save path must be passed in."

        # handle the passed object path 1) bucket/key 2) key -> key
        # _key = object_p.split("/")
        # _key = _key[min(1, len(_key) - 1) :]
        # _key = "/".join(_key)
        # assert _key.endswith(
        #     (".png", ".jpeg", ".jpg")
        # ), "Invalid file format, only aim at downloading .png, .jpeg, or .jpg."

        logger.info(f"COS断点续传 下载文件路径(key): ; 本地保存路径: {save_p}")
        if os.path.exists(save_p):
            logger.warning(f"Local file already exists, will overwrite: {save_p}")

        parent_dir = os.path.dirname(save_p)
        if not os.path.exists(parent_dir):
            logger.warning(f"Parent directory does not exist, will create: {parent_dir}")
            os.makedirs(parent_dir, exist_ok=True)

        object_exist = COS_CLIENT.object_exists(Bucket=COS_BUCKET_NAME, Key=object_p)
        if not object_exist:
            logger.error(f"COS断点续传 文件不存在: ")
            return download_success

        try:
            _download_file(object_key=object_p, dest_path=save_p)
            download_success = True
        except qcloud_cos.cos_exception.CosClientError as e:
            logger.error(f"[COS Download] Fail with client error, e: {e}")
        except qcloud_cos.cos_exception.CosServiceError as e:
            logger.error(
                f"[COS Download] Fail with server error, error_code: {e.get_error_code()}, "
                f"request id: {e.get_request_id()}, message: {e.get_error_msg()}, "
                f"HTTP code: {e.get_status_code()}, digest_msg: {e.get_digest_msg()}, URL: {e.get_resource_location()}"
            )
        except Exception as e:
            logger.error(f"[COS Download] Fail with: {e}.")

        return download_success

    # Execute the async function `body` in a separate thread,
    # won't block other coroutines running on the current event loop
    success = await asyncio.to_thread(asyncio.run, body(object_path, save_path))
    return success


def _upload_file(object_key: str, file_path: str, retries: int = 5) -> dict:
    response = {}
    for _ in range(0, max(1, retries)):
        try:
            # https://github.com/hpcaitech/Open-Sora-Serving/issues/166
            # Cannot be reproduced in dev environment, but it is a known issue in the production environment.
            # response = COS_CLIENT.upload_file(Bucket=COS_BUCKET_NAME, Key=object_key, LocalFilePath=file_path)
            # Seems a workaround to prevent the above issue
            response = COS_CLIENT.put_object_from_local_file(
                Bucket=COS_BUCKET_NAME, Key=object_key, LocalFilePath=file_path
            )
            break
        except CosClientError or CosServiceError as e:
            logger.error(e)
    return response


def get_presigned_url(object_key: str, expired: int = 300) -> Optional[str]:
    if not COS_CLIENT or not COS_BUCKET_NAME:
        logger.warning("Presigned url is not available: COS client or bucket name is not initialized.")
        return
    if not COS_CLIENT.object_exists(Bucket=COS_BUCKET_NAME, Key=object_key):
        logger.warning(f"Presigned url is not available: COS object does not exist: {object_key}")
        return
    return COS_CLIENT.get_presigned_url(
        Method="GET",
        Bucket=COS_BUCKET_NAME,
        Key=object_key,
        Expired=expired,
    )


async def cos_upload_file(
    task_id: str,
    local_path: str,
    non_blocking: bool = True,
) -> Optional[str]:
    async def body(local_path: str):
        assert COS_CLIENT, "COS client should be init first."
        assert os.path.exists(local_path), f"The file({local_path}) for the current request({task_id}) not exists."

        try:
            logger.info(
                f"COS断点续传 本地任务id: {task_id}, 上传文件路径: {local_path}"
            )
            object_key = COS_OBJECT_PREFIX + local_path.split("/")[-1]
            result = _upload_file(object_key=object_key, file_path=local_path)
            if not result:
                # If failing to upload, two-stage requests and final states of requests
                # will be affected and errored out later
                logger.warning(
                    f"COS断点续传 上传失败: 任务: {task_id}, 上传路径: {local_path}"
                )
            else:    
                print(object_key)
                url = get_presigned_url(object_key=object_key, expired=300)
                if local_path is not None and os.path.exists(local_path):
                    os.remove(local_path)
                return url
                    
        # except os error for more
        except qcloud_cos.cos_exception.CosClientError as e:
            logger.error(f"Fail with client error, e: {e}")
        except qcloud_cos.cos_exception.CosServiceError as e:
            logger.error(
                f"[COS Upload] Fail with server error, error_code: {e.get_error_code()}, "
                f"request id: {e.get_request_id()}, message: {e.get_error_msg()}, "
                f"HTTP code: {e.get_status_code()}, digest_msg: {e.get_digest_msg()}, URL: {e.get_resource_location()}"
            )
        except OSError as e:
            logger.error(f"Fail with os error: {e}.")
        except Exception as e:
            logger.error(f"Fail with unknown error: {e}")

    if non_blocking:
        # Execute the async function `body` in a separate thread,
        # won't block other coroutines running on the current event loop
        await asyncio.to_thread(asyncio.run, body(local_path))
    else:
        url = await body(local_path)
        return url
    