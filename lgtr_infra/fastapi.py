"""
    Requires the following packages to be installed:
    - fastapi
    - Other packages mentioned in `auth` module
"""

import logging
from typing import Optional

# noinspection PyUnresolvedReferences
import cachecontrol
import google.auth.crypt
import google.auth.transport.requests
import google.oauth2.service_account
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.status import HTTP_401_UNAUTHORIZED

from lgtr_infra.auth import firebase_verify_id_token

logger = logging.getLogger(__name__)

_session = google.auth.transport.requests.requests.session()
_cached_session = cachecontrol.CacheControl(_session)

http_scheme = HTTPBearer(auto_error=False)


def firebase_verify_id_token_http_creds(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(http_scheme)
):
    """
        Returns None if no credentials were provided in the request, or if the credentials could not be validated
    """
    if not creds:
        return None

    return firebase_verify_id_token(creds.credentials)


def require_firebase_verify_id_token_http_creds(
    decoded_token: Optional[HTTPAuthorizationCredentials] = Depends(firebase_verify_id_token_http_creds),
):
    """
        Convert `get_project_http_creds` into a required dependency.
        Unauthenticated requests will return a 401
    """
    if not decoded_token:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail='Could not validate credentials',
        )

    return decoded_token
