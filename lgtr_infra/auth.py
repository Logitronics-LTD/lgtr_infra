"""
    This module requires the following packages to be installed:
    - firebase-admin
    - google-auth
    - oauth2client
"""

import contextlib
import datetime
import json
import logging
from pathlib import Path
import time
from typing import Any, Callable, Optional, TypedDict

import firebase_admin.auth  # Installed by auth dependencies
import google.auth.crypt
import google.auth.transport.requests
import google.oauth2.service_account
import requests
from oauth2client.client import OAuth2Credentials, credentials_from_code

logger = logging.getLogger(__name__)


class DecodedIdTokenDict(TypedDict):
    iss: str
    aud: str
    sub: str
    iat: int
    exp: int

    email: Optional[str]
    email_verified: Optional[bool]
    sub: Optional[str]

    claims: dict[str, Any]


def firebase_verify_id_token(id_token: str, n_retries=3) -> DecodedIdTokenDict | None:
    with contextlib.suppress(Exception):
        while n_retries > 0:
            try:
                return firebase_admin.auth.verify_id_token(id_token)

            except Exception as e:
                retry_strings = ['Token used too early,', "('Connection aborted.', "]
                should_retry = any(str(e).startswith(retry_str) for retry_str in retry_strings)
                if not should_retry:
                    logger.exception('Error while verifying id-token')
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


class SignInWithRefreshTokenResponseDict(TypedDict):
    id_token: str
    refresh_token: str
    expires_in: str
    token_type: str
    user_id: str
    project_id: str


def google_sign_in_with_custom_token(
    uid: str, *, api_key: str, referer: str = None, additional_claims=None
) -> SignInWithCustomTokenResponseDict:
    """
        Create an id-token for a user with the given uid, by exchanging a custom token with the firebase auth service

        Notes:
            - API Key must have the following APIs enabled: `Identity Toolkit API`
            - API HTTP Referer might be required to be set in the API Key settings
    """

    url_sign_in_with_custom_token = 'https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken'
    custom_token = firebase_admin.auth.create_custom_token(uid, additional_claims).decode()

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


def google_sign_in_with_refresh_token(
    refresh_token: str, *, api_key: str, referer: str = None
) -> SignInWithRefreshTokenResponseDict:
    """
        Similar to `google_sign_in_with_custom_token`, but uses a refresh token instead of a custom token
    """

    url_sign_in_with_refresh_token = 'https://securetoken.googleapis.com/v1/token'
    headers = {}
    if referer:
        headers['Referer'] = referer

    res = requests.post(
        f'{url_sign_in_with_refresh_token}?key={api_key}',
        json={'grant_type': 'refresh_token', 'refresh_token': refresh_token},
        headers=headers,
    )
    res.raise_for_status()
    return res.json()


@staticmethod
def load_credentials_service_account(scopes: list[str] = None, path_secret_json=None) -> google.oauth2.service_account.Credentials:
    scopes = scopes or ['https://www.googleapis.com/auth/drive']
    return google.oauth2.service_account.Credentials.from_service_account_file(str(path_secret_json), scopes=scopes)


@staticmethod
def load_credentials_user_code(code, client_id=None, client_secret=None, scopes=None, path_secret_web_client_json=None) -> OAuth2Credentials:
    scopes = scopes or ['https://www.googleapis.com/auth/drive.file']

    if path_secret_web_client_json:
        path_secret = Path(path_secret_web_client_json)
        web_client_info = json.loads(path_secret.read_text())
        client_id = client_id or web_client_info['web']['client_id']
        client_secret = client_secret or web_client_info['web']['client_secret']

    return credentials_from_code(
        client_id=client_id, client_secret=client_secret, scope=' '.join(scopes), code=code
    )


@staticmethod
def load_credentials_user_info_obj(info: dict):
    return google.oauth2.credentials.Credentials.from_authorized_user_info(info)


class TokenProviderBase:
    def __init__(self, *, margin_sec=None):
        self.margin_sec = margin_sec if margin_sec is not None else 60 * 5
        self.dt_expires: Optional[datetime.datetime] = None

    def get_token(self) -> Any:
        raise NotImplementedError()

    def on_new_expiry(self, expires_in: float):
        self.dt_expires = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)

        if expires_in < self.margin_sec:
            self.margin_sec = expires_in / 2
            logger.warning(
                f'`expires_in` ({expires_in}) is smaller than `margin_sec` ({self.margin_sec}); '
                f'Updated to {self.margin_sec}'
            )

        self.dt_expires = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)

    def should_refresh(self):
        return (
            self.dt_expires is None or
            self.dt_expires < datetime.datetime.now() + datetime.timedelta(self.margin_sec)
        )


class TokenProvider(TokenProviderBase):
    def __init__(
        self, *, callback_token_response: Callable, callback_expires_in: Callable[[Any], float],
        callback_id_token: Callable[[Any], str], margin_sec: float = None
    ):
        super().__init__(margin_sec=margin_sec)

        self.callback_token_response = callback_token_response
        self.callback_expires_in = callback_expires_in
        self.callback_id_token = callback_id_token

        self.token_response: Optional[Any] = None

    def get_token(self):
        if self.should_refresh():
            self.token_response = self.callback_token_response()
            self.on_new_expiry(self.callback_expires_in(self.token_response))

        return self.callback_id_token(self.token_response)


class TokenProviderRefresh(TokenProviderBase):
    def __init__(
        self, *, refresh_token: str, api_key: str, referer: str = None, margin_sec: float = None
    ):
        super().__init__(margin_sec=margin_sec)

        self.refresh_token = refresh_token
        self.refresh_token_response: Optional[SignInWithRefreshTokenResponseDict] = None
        self.api_key = api_key
        self.referer = referer

    def use_refresh_token(self):
        self.refresh_token_response = google_sign_in_with_refresh_token(
            self.refresh_token, api_key=self.api_key, referer=self.referer
        )
        self.on_new_expiry(float(self.refresh_token_response['expires_in']))

    def get_token(self):
        if self.should_refresh():
            self.use_refresh_token()

        return self.refresh_token_response['id_token']
