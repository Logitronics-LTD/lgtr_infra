"""
    Requires the following packages to be installed:
    - firebase-admin
    - google-auth
"""

import contextlib
import logging
import time
from typing import Optional, TypedDict

# noinspection PyUnresolvedReferences
import cachecontrol
# noinspection PyUnresolvedReferences
import firebase_admin.auth
import google.auth.crypt
import google.auth.transport.requests
import google.oauth2.service_account
import requests

logger = logging.getLogger(__name__)

_session = google.auth.transport.requests.requests.session()
_cached_session = cachecontrol.CacheControl(_session)


class DecodedIdTokenDict(TypedDict):
    iss: str
    aud: str
    sub: str
    iat: int
    exp: int

    email: Optional[str]
    email_verified: Optional[bool]
    sub: Optional[str]


def firebase_verify_id_token(id_token: str, n_retries=3) -> DecodedIdTokenDict | None:
    with contextlib.suppress(Exception):
        while n_retries > 0:
            try:
                return firebase_admin.auth.verify_id_token(id_token)

            except Exception as e:
                retry_strings = ['Token used too early,', "('Connection aborted.', "]
                should_retry = any(str(e).startswith(retry_str) for retry_str in retry_strings)
                if not should_retry:
                    break

            n_retries -= 1
            time.sleep(1)

    return None


class SignInWithCustomTokenResponseDict(TypedDict):
    kind: str
    idToken: str
    refreshToken: str
    expiresIn: str
    isNewUser: bool


def google_sign_in_with_custom_token(uid: str, api_key: str, referer: str = None) -> SignInWithCustomTokenResponseDict:
    """
        Create an id-token for a user with the given uid, by exchanging a custom token with the firebase auth service

        Notes:
            - API Key must have the following APIs enabled: `Identity Toolkit API`
            - API HTTP Referer might be required to be set in the API Key settings
    """

    url_sign_in_with_custom_token = 'https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken'
    custom_token = firebase_admin.auth.create_custom_token(uid).decode()

    headers = {}
    if referer:
        headers['Referer'] = referer

    res = requests.post(
        f'{url_sign_in_with_custom_token}?key={api_key}',
        json={'token': custom_token, 'returnSecureToken': True},
        headers=headers,
    )
    res.raise_for_status()
    return res.json()
