import re
from datetime import datetime
from typing import Any, Dict, Iterator, Optional
from uuid import uuid4

import awswrangler as wr
import boto3
import boto3.session
import pandas as pd

from ml_lib.feature_store.offline.format import apply_sql_parameters
from ml_lib.util.metrics import incr, timing

from ..client_config import get_offline_config

REMOVE_TRAILING_SEMICOLON_REGEX = re.compile("[;\n\r\t ]+$")


def remove_trailing_semicolon(sql: str) -> str:
    return re.sub(REMOVE_TRAILING_SEMICOLON_REGEX, "", sql)


class FeatureStoreOfflineClient:
    @staticmethod
    def _get_athena_workgroup_s3_staging_dir(
        boto3_session: boto3.session.Session, workgroup: str
    ) -> str:
        client = boto3_session.client("athena")
        response = client.get_work_group(WorkGroup=workgroup)
        output_location = (
            response.get("WorkGroup", {})
            .get("Configuration", {})
            .get("ResultConfiguration", {})
            .get("OutputLocation")
        )

        if output_location is None:
            raise ValueError(
                f'Workgroup "workgroup" doesn\'t have OutputLocation configured'
            )

        return output_location

    @staticmethod
    def _build_unload_s3_output(workgroup_s3_staging_dir: str) -> str:
        return (
            "/".join(
                [
                    workgroup_s3_staging_dir.rstrip("/"),
                    "unload",
                    datetime.utcnow().date().isoformat(),
                    str(uuid4()),
                ]
            )
            + "/"
        )

    @staticmethod
    def _get_boto3_session(role_arn: Optional[str]) -> boto3.session.Session:
        if role_arn is not None:
            sts_client = boto3.client("sts", region_name="us-east-2")
            assumed_role = sts_client.assume_role(
                RoleArn=role_arn, RoleSessionName="AssumeRoleSession"
            )
            credentials = assumed_role["Credentials"]
            session = boto3.session.Session(
                region_name="us-east-2",
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
            )
        else:
            session = boto3.session.Session(region_name="us-east-2")

        return session

    @staticmethod
    def render_query(sql: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        sql = apply_sql_parameters(sql, parameters)
        sql = remove_trailing_semicolon(sql)
        return sql

    @classmethod
    def run_athena_query_pandas(
        cls, sql: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        config = get_offline_config()

        boto3_session = cls._get_boto3_session(config.role_arn)
        workgroup_s3_staging_dir = cls._get_athena_workgroup_s3_staging_dir(
            boto3_session=boto3_session, workgroup=config.workgroup
        )
        s3_output = cls._build_unload_s3_output(
            workgroup_s3_staging_dir=workgroup_s3_staging_dir
        )

        incr("athena_query_runs_total", 1)

        sql = apply_sql_parameters(sql, parameters)
        sql = remove_trailing_semicolon(sql)

        try:
            df = wr.athena.read_sql_query(
                sql=sql,
                database="default",
                workgroup=config.workgroup,
                boto3_session=boto3_session,
                data_source=config.catalog_name,
                s3_output=s3_output,
                ctas_approach=False,
                unload_approach=True,
                unload_parameters=dict(compression="snappy"),
            )
        except wr.exceptions.EmptyDataFrame:
            incr("athena_query_returned_empty_result_count", 1)
            return pd.DataFrame()

        stats: Dict[str, int] = df.query_metadata["Statistics"]

        timing("athena_query_queue_ms", stats["QueryQueueTimeInMillis"])
        timing("athena_query_execution_ms", stats["TotalExecutionTimeInMillis"])
        incr("athena_query_scanned_bytes", stats["DataScannedInBytes"])

        return df

    @classmethod
    def run_batched_athena_query_pandas(
        cls,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        rows_per_batch: int = 10_000,
    ) -> Iterator[pd.DataFrame]:
        config = get_offline_config()

        boto3_session = cls._get_boto3_session(config.role_arn)
        workgroup_s3_staging_dir = cls._get_athena_workgroup_s3_staging_dir(
            boto3_session=boto3_session, workgroup=config.workgroup
        )
        s3_output = cls._build_unload_s3_output(
            workgroup_s3_staging_dir=workgroup_s3_staging_dir
        )

        incr("athena_query_runs_total", 1)

        sql = apply_sql_parameters(sql, parameters)
        sql = remove_trailing_semicolon(sql)

        try:
            gen: Iterator[pd.DataFrame] = wr.athena.read_sql_query(
                sql=sql,
                database="default",
                workgroup=config.workgroup,
                boto3_session=boto3_session,
                data_source=config.catalog_name,
                s3_output=s3_output,
                ctas_approach=False,
                unload_approach=True,
                unload_parameters=dict(compression="snappy"),
                chunksize=rows_per_batch,
            )
        except wr.exceptions.EmptyDataFrame:
            incr("athena_query_returned_empty_result_count", 1)
            yield pd.DataFrame()
            return

        df = next(gen)

        stats: Dict[str, int] = df.query_metadata["Statistics"]

        timing("athena_query_queue_ms", stats["QueryQueueTimeInMillis"])
        timing(
            "athena_query_execution_ms",
            stats["TotalExecutionTimeInMillis"],
        )
        incr("athena_query_scanned_bytes", stats["DataScannedInBytes"])

        yield df
        yield from gen
