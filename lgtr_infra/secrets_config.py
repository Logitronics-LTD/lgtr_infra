import asyncio
from dataclasses import dataclass, field
import os
from typing import Any, Optional


@dataclass
class SecretsConfigGcp:
    """
        Config for loading secrets from GCP Secret Manager
        Must install package before using - `pip install google-cloud-secret-manager`
        For auth, set GOOGLE_APPLICATION_CREDENTIALS env var to point to a service account JSON file
    """

    project_id: str

    # Map env vars to GCP secret IDs
    load_envs_from_secrets: dict[str, str] = field(default_factory=dict)

    # Map keys to GCP secret IDs
    load_objs_from_secrets: dict[str, str] = field(default_factory=dict)

    # Secrets loaded from GCP
    env_vars: dict[str, str] = field(default_factory=dict)
    secrets: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    async def _get_secret_values(secret_ids: list[str], project_id: str) -> dict[str, str]:
        # Import locally to avoid dependency on GCP libraries.
        from google.cloud import secretmanager

        async_client = secretmanager.SecretManagerServiceAsyncClient()
        loaded_secrets = {}

        async def _load_secret(_secret_id):
            _response = await async_client.access_secret_version(
                name=f"projects/{project_id}/secrets/{_secret_id}/versions/latest"
            )
            loaded_secrets[_secret_id] = _response.payload.data.decode("UTF-8")

        await asyncio.gather(*[_load_secret(_id) for _id in secret_ids])
        return loaded_secrets

    def load_secrets(self):
        secret_ids_to_load = [
            *(self.load_envs_from_secrets or {}).values(),
            *(self.load_objs_from_secrets or {}).values(),
        ]

        loaded_secrets = asyncio.run(self._get_secret_values(secret_ids_to_load, self.project_id))
        self.env_vars = {
            _key: loaded_secrets[_id] for _key, _id in self.load_envs_from_secrets.items()
        }

        self.secrets = {
            _key: loaded_secrets[_id] for _key, _id in self.load_objs_from_secrets.items()
        }

        os.environ.update(self.env_vars)


@dataclass
class SecretsConfigAirtable:
    """
        Config for loading secrets from Airtable
    """

    api_key: str
    base_id: str
    table_name: str

    load_keys: Optional[list[str]] = None
    records: dict[str, Any] = field(default_factory=dict)
    env_vars: dict[str, str] = field(default_factory=dict)

    field_key = 'key'
    field_value = 'value'
    field_env_var = 'env_var'

    is_loaded = False

    def load_secrets(self, reload=False):
        if self.is_loaded and not reload:
            return

        # Import locally to avoid dependency on Airtable library
        from pyairtable import Table

        table = Table(api_key=self.api_key, base_id=self.base_id, table_name=self.table_name)
        records = [r['fields'] for r in table.all()]

        keys = self.load_keys
        if keys is None:
            keys = [
                record[self.field_key] for record in records
                if record[self.field_key]
            ]

        set_keys = set(keys)

        self.records = {
            record[self.field_key]: record[self.field_value]
            for record in records
            if record[self.field_key] in set_keys
        }

        self.env_vars = {
            record[self.field_env_var]: record[self.field_value]
            for record in records
            if record.get(self.field_env_var) and record.get(self.field_key) in set_keys
        }

        os.environ.update(self.env_vars)
        self.is_loaded = True
