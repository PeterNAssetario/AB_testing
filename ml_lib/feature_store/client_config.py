from typing import Any, Optional, cast

from pydantic import BaseSettings, Field, PostgresDsn, ValidationError, validator
from pydantic.fields import FieldInfo

from ml_lib.feature_store.errors import (
    OfflineClientNotConfiguredError,
    OnlineClientNotConfiguredError,
    ProjectScopeNotConfiguredError,
)


class FeatureStoreClientOfflineConfig(BaseSettings):
    workgroup: str = Field(..., env="FEATURE_STORE_OFFLINE_ATHENA_WORK_GROUP")
    role_arn: Optional[str] = Field(
        default=None, env="FEATURE_STORE_OFFLINE_ATHENA_ROLE_ARN"
    )
    catalog_name: Optional[str] = Field(
        default=None, env="FEATURE_STORE_OFFLINE_ATHENA_CATALOG_NAME"
    )

    @validator("*", pre=True)
    def not_none(cls, value: Any, field: FieldInfo) -> Any:
        # Force field default value if None is passed explicitly
        if field.default is not None and value is None:
            return field.default
        else:
            return value


class FeatureStoreClientOnlineConfig(BaseSettings):
    pg_url: PostgresDsn = Field(..., env="FEATURE_STORE_ONLINE_PG_URL")
    min_connections: Optional[int] = Field(
        default=0, env="FEATURE_STORE_ONLINE_PG_MIN_CONNECTIONS"
    )
    max_connections: Optional[int] = Field(
        default=5, env="FEATURE_STORE_ONLINE_PG_MAX_CONNECTIONS"
    )

    @validator("*", pre=True)
    def not_none(cls, value: Any, field: FieldInfo) -> Any:
        # Force field default value if None is passed explicitly
        if field.default is not None and value is None:
            return field.default
        else:
            return value


class FeatureStoreProjectScope(BaseSettings):
    company_id: str = Field(..., env="META_COMPANY_ID")
    project_id: str = Field(..., env="META_PROJECT_ID")

    # TODO - check if required
    def __hash__(self) -> int:
        return hash((self.company_id, self.project_id))


_offline_config: Optional[FeatureStoreClientOfflineConfig] = None
_online_config: Optional[FeatureStoreClientOnlineConfig] = None
_project_scope: Optional[FeatureStoreProjectScope] = None


def configure_offline_feature_store(
    *,
    workgroup: str,
    role_arn: Optional[str] = None,
    catalog_name: Optional[str] = None
) -> None:
    global _offline_config
    try:
        _offline_config = FeatureStoreClientOfflineConfig(
            workgroup=workgroup, role_arn=role_arn, catalog_name=catalog_name
        )
    except ValidationError as error:
        raise OfflineClientNotConfiguredError(error)


def configure_online_feature_store(
    *,
    pg_url: str,
    min_connections: Optional[int] = None,
    max_connections: Optional[int] = None
) -> None:
    global _online_config
    try:
        _online_config = FeatureStoreClientOnlineConfig(
            pg_url=cast(PostgresDsn, pg_url),
            min_connections=min_connections,
            max_connections=max_connections,
        )
    except ValidationError as error:
        raise OnlineClientNotConfiguredError(error)


def configure_feature_store_project_scope(*, company_id: str, project_id: str) -> None:
    global _project_scope
    try:
        _project_scope = FeatureStoreProjectScope(
            company_id=company_id, project_id=project_id
        )
    except ValidationError as error:
        raise ProjectScopeNotConfiguredError(error)


def get_offline_config() -> FeatureStoreClientOfflineConfig:
    # TODO - mutex
    global _offline_config
    if _offline_config is None:
        try:
            _offline_config = FeatureStoreClientOfflineConfig()
        except ValidationError as error:
            raise OfflineClientNotConfiguredError(error)

    return _offline_config


def get_online_config() -> FeatureStoreClientOnlineConfig:
    # TODO mutex
    global _online_config
    if _online_config is None:
        try:
            _online_config = FeatureStoreClientOnlineConfig()
        except ValidationError as error:
            raise OnlineClientNotConfiguredError(error)

    return _online_config


def get_project_scope() -> FeatureStoreProjectScope:
    # TODO mutex
    global _project_scope
    if _project_scope is None:
        try:
            _project_scope = FeatureStoreProjectScope()
        except ValidationError as error:
            raise ProjectScopeNotConfiguredError(error)

    return _project_scope


def clear_offline_config() -> None:
    # TODO - mutex
    global _offline_config
    _offline_config = None


def clear_online_config() -> None:
    # TODO mutex
    global _online_config
    _online_config = None


def clear_project_scope() -> None:
    # TODO mutex
    global _project_scope
    _project_scope = None
