from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from wybra.db.provisioning.core import (
    DatabaseFamily,
    DatabaseProvisioningConfigurationError,
    DatabaseProvisioningOperationError,
    ProvisioningContext,
)
from wybra.db.settings import (
    AwsClientSettings,
    AwsManagedDatabaseSettings,
    AwsManagedTarget,
)

_DEFAULT_ROLE_SESSION_NAME = "wybra-database-provisioning"


@dataclass(frozen=True, slots=True)
class AwsManagedDatabaseMetadata:
    managed: AwsManagedTarget
    identifier: str
    engine: str
    endpoint: str | None
    port: int | None
    arn: str | None = None
    region: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "managed",
            _normalise_managed_target(self.managed),
        )

    @property
    def account_id(self) -> str | None:
        return _account_id_from_arn(self.arn)

    @property
    def partition(self) -> str | None:
        return _partition_from_arn(self.arn)


class AwsRdsMetadataClient(Protocol):
    def describe(
        self,
        target: AwsManagedDatabaseSettings,
    ) -> AwsManagedDatabaseMetadata: ...


class Boto3RdsMetadataClient:
    def __init__(
        self,
        *,
        import_module: Callable[[str], Any] = importlib.import_module,
    ) -> None:
        self._import_module = import_module

    def describe(
        self,
        target: AwsManagedDatabaseSettings,
    ) -> AwsManagedDatabaseMetadata:
        try:
            if target.managed == "rds":
                identifier = _required_db_instance_identifier(target)
                client = self._rds_client(target.client)
                response = client.describe_db_instances(
                    DBInstanceIdentifier=identifier,
                )
                instances = response.get("DBInstances") or ()
                if not instances:
                    raise DatabaseProvisioningConfigurationError(
                        "AWS RDS target was not found."
                    )
                return _metadata_from_instance(
                    instances[0], region=target.client.region
                )

            identifier = _required_cluster_identifier(target)
            client = self._rds_client(target.client)
            response = client.describe_db_clusters(
                DBClusterIdentifier=identifier,
            )
            clusters = response.get("DBClusters") or ()
            if not clusters:
                raise DatabaseProvisioningConfigurationError(
                    "AWS Aurora target was not found."
                )
            return _metadata_from_cluster(clusters[0], region=target.client.region)
        except DatabaseProvisioningConfigurationError:
            raise
        except Exception as exc:
            raise DatabaseProvisioningOperationError(
                "Failed to describe AWS managed database target."
            ) from exc

    def _rds_client(self, settings: AwsClientSettings) -> Any:
        session = self._session(settings)
        return session.client("rds", region_name=settings.region)

    def _session(self, settings: AwsClientSettings) -> Any:
        boto3 = _import_boto3(self._import_module)
        session_kwargs: dict[str, str] = {}
        if settings.profile is not None:
            session_kwargs["profile_name"] = settings.profile
        if settings.region is not None:
            session_kwargs["region_name"] = settings.region
        session = boto3.session.Session(**session_kwargs)
        if settings.role_arn is None:
            return session
        sts = session.client("sts", region_name=settings.region)
        assume_role_args = {
            "RoleArn": settings.role_arn,
            "RoleSessionName": (
                settings.role_session_name or _DEFAULT_ROLE_SESSION_NAME
            ),
        }
        external_id = settings.external_id
        if external_id is not None:
            assume_role_args["ExternalId"] = external_id
        credentials = sts.assume_role(**assume_role_args)["Credentials"]
        return boto3.session.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=settings.region,
        )


def validate_aws_managed_database_context(
    context: ProvisioningContext,
    *,
    metadata_client: AwsRdsMetadataClient | None = None,
) -> ProvisioningContext:
    target = _aws_target_for_context(context)
    if target is None:
        return context
    metadata = (metadata_client or Boto3RdsMetadataClient()).describe(target)
    family = database_family_for_aws_engine(metadata.engine)
    if context.family != family:
        raise DatabaseProvisioningConfigurationError(
            "AWS managed database engine does not match configured backend."
        )
    _validate_metadata_matches_target(target, metadata, context)
    return context


def _normalise_managed_target(value: object) -> AwsManagedTarget:
    if isinstance(value, str):
        target = value.strip().lower()
        if target in {"rds", "aurora"}:
            return target
    raise DatabaseProvisioningConfigurationError(
        "AWS managed database metadata target must be rds or aurora."
    )


def _required_db_instance_identifier(
    target: AwsManagedDatabaseSettings,
) -> str:
    if target.db_instance_identifier is not None:
        return target.db_instance_identifier
    raise DatabaseProvisioningConfigurationError(
        "AWS RDS target requires db_instance_identifier."
    )


def _required_cluster_identifier(
    target: AwsManagedDatabaseSettings,
) -> str:
    if target.cluster_identifier is not None:
        return target.cluster_identifier
    raise DatabaseProvisioningConfigurationError(
        "AWS Aurora target requires cluster_identifier."
    )


def database_family_for_aws_engine(engine: str) -> DatabaseFamily:
    normalised = engine.strip().lower()
    if normalised in {"postgres", "postgresql", "aurora-postgresql"}:
        return "postgresql"
    if normalised in {"mysql", "aurora", "aurora-mysql"}:
        return "mysql"
    if normalised == "mariadb":
        return "mariadb"
    if normalised.startswith("sqlserver-"):
        return "mssql"
    if normalised.startswith("oracle-"):
        raise DatabaseProvisioningConfigurationError(
            "AWS managed Oracle databases are not supported."
        )
    raise DatabaseProvisioningConfigurationError(
        f"Unsupported AWS managed database engine: {engine}."
    )


def _aws_target_for_context(
    context: ProvisioningContext,
) -> AwsManagedDatabaseSettings | None:
    if context.provisioning_connection is not None:
        target = context.provisioning_connection.aws
        if target is not None:
            return target
    return context.runtime_connection.aws


def _validate_metadata_matches_target(
    target: AwsManagedDatabaseSettings,
    metadata: AwsManagedDatabaseMetadata,
    context: ProvisioningContext,
) -> None:
    if metadata.managed != target.managed:
        raise DatabaseProvisioningConfigurationError(
            "AWS managed database target type mismatch."
        )
    expected_identifier = (
        target.db_instance_identifier
        if target.managed == "rds"
        else target.cluster_identifier
    )
    if metadata.identifier != expected_identifier:
        raise DatabaseProvisioningConfigurationError(
            "AWS managed database target identifier mismatch."
        )
    if target.engine is not None and metadata.engine.lower() != target.engine.lower():
        raise DatabaseProvisioningConfigurationError(
            "AWS managed database engine mismatch."
        )
    if (
        target.client.account_id is not None
        and metadata.account_id is not None
        and target.client.account_id != metadata.account_id
    ):
        raise DatabaseProvisioningConfigurationError(
            "AWS managed database account mismatch."
        )
    if (
        target.client.partition is not None
        and metadata.partition is not None
        and target.client.partition != metadata.partition
    ):
        raise DatabaseProvisioningConfigurationError(
            "AWS managed database partition mismatch."
        )
    expected_endpoint = _expected_endpoint(target, context)
    if (
        expected_endpoint is not None
        and metadata.endpoint is not None
        and expected_endpoint.lower() != metadata.endpoint.lower()
    ):
        raise DatabaseProvisioningConfigurationError(
            "AWS managed database endpoint mismatch."
        )
    expected_port = _expected_port(target, context)
    if expected_port is not None and metadata.port is not None:
        if expected_port != metadata.port:
            raise DatabaseProvisioningConfigurationError(
                "AWS managed database port mismatch."
            )


def _expected_endpoint(
    target: AwsManagedDatabaseSettings,
    context: ProvisioningContext,
) -> str | None:
    if target.endpoint is not None:
        return target.endpoint
    host = context.runtime_connection.credentials.get("host")
    return host if isinstance(host, str) and host.strip() else None


def _expected_port(
    target: AwsManagedDatabaseSettings,
    context: ProvisioningContext,
) -> int | None:
    if target.port is not None:
        return target.port
    port = context.runtime_connection.credentials.get("port")
    return port if isinstance(port, int) else None


def _metadata_from_instance(
    instance: Mapping[str, Any],
    *,
    region: str | None,
) -> AwsManagedDatabaseMetadata:
    endpoint = instance.get("Endpoint") or {}
    return AwsManagedDatabaseMetadata(
        managed="rds",
        identifier=str(instance.get("DBInstanceIdentifier") or ""),
        engine=str(instance.get("Engine") or ""),
        endpoint=_endpoint_address(endpoint),
        port=_endpoint_port(endpoint),
        arn=_optional_string(instance.get("DBInstanceArn")),
        region=region,
    )


def _metadata_from_cluster(
    cluster: Mapping[str, Any],
    *,
    region: str | None,
) -> AwsManagedDatabaseMetadata:
    return AwsManagedDatabaseMetadata(
        managed="aurora",
        identifier=str(cluster.get("DBClusterIdentifier") or ""),
        engine=str(cluster.get("Engine") or ""),
        endpoint=_optional_string(cluster.get("Endpoint")),
        port=_optional_int(cluster.get("Port")),
        arn=_optional_string(cluster.get("DBClusterArn")),
        region=region,
    )


def _endpoint_address(endpoint: object) -> str | None:
    if isinstance(endpoint, Mapping):
        return _optional_string(endpoint.get("Address"))
    return None


def _endpoint_port(endpoint: object) -> int | None:
    if isinstance(endpoint, Mapping):
        return _optional_int(endpoint.get("Port"))
    return None


def _optional_string(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _account_id_from_arn(arn: str | None) -> str | None:
    if arn is None:
        return None
    parts = arn.split(":", maxsplit=5)
    return parts[4] if len(parts) >= 5 and parts[4] else None


def _partition_from_arn(arn: str | None) -> str | None:
    if arn is None:
        return None
    parts = arn.split(":", maxsplit=2)
    return parts[1] if len(parts) >= 2 and parts[1] else None


def _import_boto3(import_module: Callable[[str], Any]) -> Any:
    try:
        return import_module("boto3")
    except ImportError as exc:
        raise DatabaseProvisioningConfigurationError(
            "AWS RDS validation requires the wybra[aws] optional dependency."
        ) from exc


__all__ = (
    "AwsManagedDatabaseMetadata",
    "AwsRdsMetadataClient",
    "Boto3RdsMetadataClient",
    "database_family_for_aws_engine",
    "validate_aws_managed_database_context",
)
