from ab_testing_evaluation.testing import AbTestEvaluationConfig, run_ab_testing

if __name__ == "__main__":
    config = AbTestEvaluationConfig(
        company_id='tilting_point_mjs4k',
        project_id='spongebob_x7d9q',
        test_name='spongebob_test_1',
        ab_test_id='test',
        start_date='2022-11-17',
        end_date='2022-11-18',
        winsorized='True',
        personalized='False',
        datapoint_type='one_datapoint_per_user_per_meta_date',
        n_days_spend='7',
        min_first_login_date='2022-11-17',
        max_first_login_date='2028-1-31',
        variant_name_1='P',
        variant_name_2='C',
        a_prior_beta_1='1',
        a_prior_beta_2='1',
        b_prior_beta_1='1',
        b_prior_beta_2='1',
        m_prior_1='1.870407',
        m_prior_2='1.870407',
        a_prior_ig_1='2.313530563844',
        a_prior_ig_2='0.7354956768576817',
        b_prior_ig_1='2.313530563844',
        b_prior_ig_2='0.7354956768576817',
        w_prior_1='10',
        w_prior_2='10',
        initial_test_start_date='2022-11-17',
        output_bucket='-',
        output_key='-',
    )
    ab_testing_output = run_ab_testing(config)

    print(ab_testing_output)
    print('DONE')
