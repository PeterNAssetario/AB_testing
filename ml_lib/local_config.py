from typing import Optional

from pydantic import PostgresDsn

from ml_lib.feature_store import (
    configure_feature_store_project_scope,
    configure_offline_feature_store,
    configure_online_feature_store,
)


class LocalConfig:
    @staticmethod
    def configure_feature_store_project_scope(
        *, company_id: str, project_id: str
    ) -> None:
        configure_feature_store_project_scope(
            company_id=company_id, project_id=project_id
        )

    @staticmethod
    def configure_online_feature_store(
        *,
        pg_url: PostgresDsn,
        min_connections: Optional[int] = None,
        max_connections: Optional[int] = None
    ) -> None:
        configure_online_feature_store(
            pg_url=pg_url,
            min_connections=min_connections,
            max_connections=max_connections,
        )

    @staticmethod
    def configure_offline_feature_store(
        *,
        workgroup: str,
        role_arn: Optional[str] = None,
        catalog_name: Optional[str] = None
    ) -> None:
        configure_offline_feature_store(
            workgroup=workgroup, role_arn=role_arn, catalog_name=catalog_name
        )
