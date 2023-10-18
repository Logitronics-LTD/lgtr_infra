"""
    This module requires the following packages to be installed:
    - google-auth = "^2.23.3"
    - google-api-python-client
"""
import abc
import copy
import io
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import cloudpathlib
import cloudpathlib.client
import google.oauth2.credentials
import google.oauth2.service_account
import httpx
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from retry import retry

logger = logging.getLogger()


class TemporaryException(Exception):
    pass


retry_gdrive = retry(
    exceptions=(TemporaryException,), tries=4, delay=1, backoff=2, logger=logger
)


def raise_if_temporary_exception(exception: Exception):
    from googleapiclient.errors import HttpError  # Prevent circular import

    if isinstance(exception, HttpError) and exception.error_details[0]['reason'] == 'userRateLimitExceeded':
        raise TemporaryException(
            'Google API rate limit exceeded') from exception


@retry_gdrive
def gapi_batch_wrapper(gapi_service, query_objects: list):

    response_objects = []
    exceptions = []

    def batch_callback(rid, resp, exc):
        if exc:
            exceptions.append(exc)

        response_objects.append(resp if resp else None)

    batch = gapi_service.new_batch_http_request(batch_callback)
    for query_object in query_objects:
        batch.add(query_object)

    batch.execute()

    for exception in exceptions:
        raise_if_temporary_exception(exception)

    if len(exceptions) > 0:
        raise Exception(
            'Batch request failed: \n' +
            '\n'.join([str(e) for e in exceptions])
        )

    return response_objects


@dataclass
class FileMeta:
    id: Optional[str] = None
    name: Optional[str] = None
    parent_ids: Optional[list[str]] = None
    _raw: Optional[Any] = None


class IoAdapterBase(abc.ABC):
    def list_dir_contents(self, folder_id: str) -> dict[str, FileMeta]:
        raise NotImplementedError()

    def get_file_bytes(self, file_id: str) -> bytes:
        raise NotImplementedError()

    def get_parent_folder(self, object_id: str) -> FileMeta:
        return self.get_parent_folders([object_id])[0]

    def get_parent_folders(self, object_ids: list[str]) -> list[FileMeta]:
        raise NotImplementedError()

    def overwrite_file_bytes(self, folder_id: str, file_meta: FileMeta, file_bytes: bytes) -> FileMeta:
        raise NotImplementedError()

    def raw_meta_to_file_meta(self, raw_meta: Any) -> FileMeta:
        raise NotImplementedError()

    def raw_list_to_file_meta(self, raw_list: list[Any]) -> dict[str, FileMeta]:
        result = dict()
        for raw_meta in raw_list:
            file_meta = self.raw_meta_to_file_meta(raw_meta)
            result[file_meta.id] = file_meta

        return result

    def get_meta(self, object_id: list[str]) -> FileMeta:
        return self.get_metas([object_id])[0]

    def get_metas(self, object_ids: list[str]) -> list[FileMeta]:
        return [
            FileMeta(id=obj_id, name=obj_id)
            for obj_id in object_ids
        ]

    def get_or_create_subfolder(self, parent_folder_id: str, folder_meta: FileMeta) -> FileMeta:
        raise NotImplementedError()


class IoAdapterPath(IoAdapterBase):
    def list_dir_contents(self, folder_id: str) -> dict[str, FileMeta]:
        return self.raw_list_to_file_meta(
            list(Path(folder_id).glob('*'))
        )

    def get_file_bytes(self, file_id: str) -> bytes:
        return Path(file_id).read_bytes()

    def get_parent_folders(self, object_ids: list[str]) -> list[FileMeta]:
        return [
            self.raw_meta_to_file_meta(Path(object_id).parent)
            for object_id in object_ids
        ]

    def overwrite_file_bytes(self, folder_id: str, file_meta: FileMeta, file_bytes: bytes) -> FileMeta:
        path_file = Path(folder_id) / file_meta.name
        path_file.write_bytes(file_bytes)
        file_meta = copy.deepcopy(file_meta)
        file_meta.id = str(path_file)
        return file_meta

    def raw_meta_to_file_meta(self, raw_meta: Path) -> FileMeta:
        return FileMeta(
            id=str(raw_meta),
            name=raw_meta.name
        )

    def get_or_create_subfolder(self, parent_folder_id: str, folder_meta: FileMeta) -> FileMeta:
        dir_subfolder = Path(parent_folder_id) / folder_meta.name
        if not dir_subfolder.is_dir():
            dir_subfolder.mkdir(parents=True, exist_ok=True)

        return FileMeta(
            id=str(dir_subfolder),
            name=dir_subfolder.name
        )


class IoAdapterCloudPath(IoAdapterBase):
    def __init__(self, client: cloudpathlib.client.Client):
        super().__init__()

        self.client = client

    def cloud_path_obj(self, object_id: str) -> cloudpathlib.CloudPath:
        return self.client.CloudPath(object_id)

    def list_dir_contents(self, folder_id: str) -> dict[str, FileMeta]:
        return self.raw_list_to_file_meta(
            list(self.cloud_path_obj(folder_id).glob('*'))
        )

    def get_file_bytes(self, file_id: str) -> bytes:
        return self.cloud_path_obj(file_id).read_bytes()

    def get_parent_folders(self, object_ids: list[str]) -> list[FileMeta]:
        return [
            self.raw_meta_to_file_meta(self.cloud_path_obj(object_id).parent)
            for object_id in object_ids
        ]

    def overwrite_file_bytes(self, folder_id: str, file_meta: FileMeta, file_bytes: bytes) -> FileMeta:
        path_file = self.cloud_path_obj(folder_id) / file_meta.name
        path_file.write_bytes(file_bytes)
        file_meta = copy.deepcopy(file_meta)
        file_meta.id = str(path_file)
        return file_meta

    def raw_meta_to_file_meta(self, raw_meta: Path) -> FileMeta:
        return FileMeta(
            id=str(raw_meta),
            name=raw_meta.name
        )

    def get_or_create_subfolder(self, parent_folder_id: str, folder_meta: FileMeta) -> FileMeta:
        dir_subfolder = self.cloud_path_obj(
            parent_folder_id) / folder_meta.name
        if not dir_subfolder.is_dir():
            dir_subfolder.mkdir(parents=True, exist_ok=True)

        return FileMeta(
            id=str(dir_subfolder),
            name=dir_subfolder.name
        )


class IoAdapterGdrive(IoAdapterBase):
    def __init__(
        self, creds: google.oauth2.service_account.Credentials = None, adapter_create: 'IoAdapterGdrive' = None
    ):
        self.creds = creds
        self.service = build('drive', 'v3', credentials=self.creds)
        self.io_create = adapter_create

    @staticmethod
    def load_credentials_user_refresh_token(refresh_token, client_id=None, client_secret=None, scopes=None, path_secret_web_client_json=None):
        if path_secret_web_client_json:
            path_secret = Path(path_secret_web_client_json)
            web_client_info = json.loads(path_secret.read_text())
            client_id = web_client_info['web']['client_id']
            client_secret = web_client_info['web']['client_secret']

        return google.oauth2.credentials.Credentials.from_authorized_user_info(
            {
                'refresh_token': refresh_token,
                'client_id': client_id,
                'client_secret': client_secret,
            },
            scopes=scopes
        )

    def raw_meta_to_file_meta(self, raw_meta: dict) -> FileMeta:
        return FileMeta(
            id=raw_meta['id'],
            name=raw_meta['name'],
            parent_ids=raw_meta.get('parents', []),
            _raw=raw_meta
        )

    @retry_gdrive
    def list_gdrive(self, **kwargs):
        # Iterate all pages in response
        file_list = []
        page_token = None
        while True:
            fields = kwargs.pop('fields', 'nextPageToken, files(id, name)')
            response = self.service.files().list(
                fields=fields, pageToken=page_token, **kwargs
            ).execute()
            file_list.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        return self.raw_list_to_file_meta(file_list)

    def list_dir_contents(self, folder_id):
        if folder_id == 'sharedWithMe':
            q = "sharedWithMe=true and trashed=false"
        else:
            q = f"'{folder_id}' in parents and trashed=false"

        return self.list_gdrive(q=q)

    @retry_gdrive
    def get_file_bytes(self, file_id) -> bytes:
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()

            fh.seek(0)
            return fh.read()
        except Exception as e:
            raise_if_temporary_exception(e)

    def get_metas(self, object_ids: list[str]) -> list[FileMeta]:
        raw_results = gapi_batch_wrapper(
            self.service,
            [self.service.files().get(fileId=object_id)
             for object_id in object_ids]
        )

        return [self.raw_meta_to_file_meta(r) for r in raw_results]

    def set_modified_date(self, object_ids: list[str], datetimes: list[datetime]):
        if self.io_create is not None:
            return self.io_create.set_modified_date(object_ids, datetimes)

        raw_results = gapi_batch_wrapper(
            self.service,
            [
                self.service.files().patch(
                    fileId=object_id, setModifiedDate=True, file={'modifiedDate': dt}, fields='modifiedDate'
                )
                for object_id, dt in zip(object_ids, datetimes)
            ]
        )

        return [self.raw_meta_to_file_meta(r) for r in raw_results]

    def get_parent_folders(self, object_ids: list[str]) -> list[FileMeta]:
        """
            Note about GDrive changes (Source: https://developers.google.com/drive/api/v3/ref-single-parent) -
                Beginning Sept. 30, 2020, you will no longer be able to place a file in multiple parent folders;
                every file must have exactly one parent folder location.
        """
        response_objects = gapi_batch_wrapper(
            self.service,
            [self.service.files().get(fileId=object_id, fields='parents')
             for object_id in object_ids]
        )

        for response in response_objects:
            parent_ids = response.get('parents', [])
            if len(parent_ids) != 1:
                raise Exception(
                    'Every file must have exactly one parent folder location')

        time.sleep(0.5)
        return self.get_metas([response['parents'][0] for response in response_objects])

    @retry_gdrive
    def overwrite_file_bytes(self, folder_id: str, file_meta: FileMeta, file_bytes: bytes) -> FileMeta:
        """
            :param folder_id: Only used if file_meta.id is None
        """
        file_id = file_meta.id
        if file_id is None:
            response = self.service.files().list(
                q=f"'{folder_id}' in parents and name = '{file_meta.name}' and trashed = false", pageSize=1
            ).execute()

            fl = response.get('files', [])
            if len(fl) > 0:
                file_id = fl[0]['id']
                logger.info(
                    f'Updating existing file: {file_meta.name} ({file_id})')

        file_metadata = {
            'name': file_meta.name,
        }

        service_create = self.service
        if self.io_create is not None:
            service_create = self.io_create.service

        media = MediaIoBaseUpload(
            BytesIO(file_bytes), mimetype='application/octet-stream')
        if file_id is None:
            file_metadata['parents'] = [folder_id]
            response = service_create.files().create(
                body=file_metadata, media_body=media).execute()
        else:
            response = self.service.files().update(
                fileId=file_id, body=file_metadata, media_body=media).execute()

        return self.raw_meta_to_file_meta(response)

    @retry_gdrive
    def add_user_permission(self, file_id: str, new_user_email_share: str, is_read_only=False):
        self.list_dir_contents(folder_id=file_id)
        self.service.permissions().create(
            fileId=file_id,
            body={
                'role': 'reader' if is_read_only else 'writer',
                'type': 'user',
                'emailAddress': new_user_email_share,
            }
        ).execute()

    def get_permissions(self, file_ids: list[str]):
        raw_results = gapi_batch_wrapper(
            self.service,
            [
                self.service.permissions().list(
                    fileId=file_id, fields='permissions(id,emailAddress,type,role)')
                for file_id in file_ids
            ]
        )

        results = []
        for response in raw_results:
            current_permissions = {}
            for permission in response.get('permissions', []):
                p_email = permission.get('emailAddress')
                p_role = permission.get('role')
                if p_email:
                    current_permissions[p_email.lower()] = p_role

            results.append(current_permissions)

        return results

    def check_permissions_read(self, file_ids: list[str], user_email: str):
        results = []
        for file_permissions in self.get_permissions(file_ids=file_ids):
            is_allowed = False
            p_role = file_permissions.get(user_email.lower())
            if p_role in {'reader', 'writer', 'owner'}:
                is_allowed = True

            results.append(is_allowed)

        return results

    @retry_gdrive
    def create_subfolder(self, parent_folder_id: str, folder_meta: FileMeta) -> FileMeta:
        if self.io_create is not None:
            return self.io_create.create_subfolder(parent_folder_id, folder_meta)

        file_metadata = {
            'name': folder_meta.name,
            'parents': [parent_folder_id],
            'mimeType': 'application/vnd.google-apps.folder',
        }
        response = self.service.files().create(body=file_metadata).execute()
        return self.raw_meta_to_file_meta(response)

    @retry_gdrive
    def create_subfolders(self, parent_folder_ids: list[str], folder_names: list[str]) -> list[FileMeta]:
        if self.io_create is not None:
            return self.io_create.create_subfolders(parent_folder_ids, folder_names)

        batch_queries = []
        for parent_folder_id, folder_name in zip(parent_folder_ids, folder_names):
            file_metadata = {
                'name': folder_name,
                'parents': [parent_folder_id],
                'mimeType': 'application/vnd.google-apps.folder',
            }
            batch_queries.append(
                self.service.files().create(body=file_metadata))

        responses = gapi_batch_wrapper(self.service)

        return [
            self.raw_meta_to_file_meta(response)
            for response in responses
        ]

    def get_subfolder(self, parent_folder_id: str, folder_meta: FileMeta) -> Optional[FileMeta]:
        return self.get_object_in_folder_ids([parent_folder_id], [folder_meta], is_folder=True)[0]

    def get_object_in_folder_ids(
        self, parent_folder_ids: list[str], metas: list[FileMeta], is_folder: bool
    ) -> list[Optional[FileMeta]]:
        """
            Elementwise batch:
                Given parent folder id, and name (in FileMeta)
                Find object matching name in parent folder id
                (Assumes that there is only one object with the same name in the same folder)
        """

        batch_queries = []
        for parent_folder_id, folder_meta in zip(parent_folder_ids, metas):
            query = \
                f"""
                    '{parent_folder_id}' in parents and
                    name='{folder_meta.name}' and
                    trashed=false
                """

            if is_folder:
                query += ' and mimeType="application/vnd.google-apps.folder"'

            batch_queries.append(
                self.service.files().list(
                    q=query,
                    fields='nextPageToken, files(id, name, parents)'
                )
            )

        raw_results = gapi_batch_wrapper(self.service, batch_queries)

        results = []
        for response in raw_results:
            files = response.get('files', [])
            if len(files) > 0:
                results.append(self.raw_meta_to_file_meta(files[0]))
            else:
                results.append(None)

        return results

    def get_or_create_subfolder(self, parent_folder_id: str, folder_meta: FileMeta) -> FileMeta:
        subfolder_meta = self.get_subfolder(
            parent_folder_id=parent_folder_id, folder_meta=folder_meta)
        if subfolder_meta:
            return subfolder_meta
        else:
            return self.create_subfolder(parent_folder_id, folder_meta)

    def get_thumbnail_urls(self, file_ids: list[str]) -> list[Optional[str]]:
        raw_results = gapi_batch_wrapper(
            self.service,
            [
                self.service.files().get(fileId=file_id, fields='id, thumbnailLink')
                for file_id in file_ids
            ]
        )

        return [
            response['thumbnailLink']
            for response in raw_results
        ]
        
    @staticmethod
    def download_bytes(urls: list[str], max_workers: int = None) -> list[Optional[bytes]]:
        # Download contents using httpx thread-pool
        http_response_to_retry = {408,425,429,500,502,503,504}

        @retry_gdrive
        def download_file(url: str):
            with httpx.Client() as client:
                response = client.get(url)
                if response.status_code != 200:
                    if response.status_code in http_response_to_retry:
                        raise TemporaryException(f'HTTP {response.status_code} error')
                    else:
                        return None
                    
                return response.content
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = [executor.submit(download_file, url) for url in urls]
            downloaded_files = [task.result() for task in as_completed(tasks)]

        return downloaded_files