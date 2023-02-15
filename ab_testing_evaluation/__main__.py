from testing import AbTestEvaluationConfig, run_ab_testing, upload_output_to_s3


if __name__ == "__main__":
    config = AbTestEvaluationConfig()
    ab_testing_output = run_ab_testing(config)
    upload_output_to_s3(
        output=ab_testing_output,
        bucket=config.output_bucket,
        key=config.output_key
    )
