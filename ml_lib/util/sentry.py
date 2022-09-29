import os
from typing import Any, Dict, Optional

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration


def configure_sentry() -> None:
    sentry_sdk.init(
        dsn=os.environ.get("SENTRY_DSN"),
        environment="production",  # TODO - env
        traces_sample_rate=0.0,
        integrations=[
            *(
                [AwsLambdaIntegration()]
                if os.getenv("LAMBDA_TASK_ROOT") is not None
                else []
            )
        ],
    )


def capture_exception(
    error: BaseException,
    extra: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> None:
    print(error)
    with sentry_sdk.push_scope() as scope:
        if extra is not None:
            scope.set_context("extra", extra)
        if tags is not None:
            for key, value in tags.items():
                scope.set_tag(key, value)
        sentry_sdk.capture_exception(error)
