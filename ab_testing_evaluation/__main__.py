from testing import AbTestEvaluationConfig, run_ab_testing, upload_output_to_s3
from ml_lib.util.sentry import configure_sentry

if __name__ == "__main__":
    configure_sentry()

    config = AbTestEvaluationConfig()
    ab_testing_output = run_ab_testing(config)
    upload_output_to_s3(
        output=ab_testing_output,
        bucket=config.output_bucket,
        key=config.output_key
    )
