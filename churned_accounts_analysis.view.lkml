view: churned_accounts_analysis {
  derived_table: {
    sql: select
       months_before_churn, --include days from churn
       plan_segment,
       MIN(prior_cent) AS prior_cent_minimum,
       AVG(prior_cent_q1) AS prior_cent_q1,
       AVG(prior_cent_median) AS prior_cent_median,
       AVG(prior_cent_q3) AS prior_cent_q3,
       MAX(prior_cent) AS prior_cent_maximum,
       MIN(prior_month_stats) AS prior_month_stats_minimum,
       AVG(prior_months_stats_q1) AS prior_months_stats_q1,
       AVG(prior_months_stats_median) AS prior_month_stats_median,
       AVG(prior_months_stats_q3) AS prior_months_stats_q3,
       MAX(prior_month_stats) AS prior_month_stats_maximum,
       ROUND(MIN(z_score_usage),2) AS z_score_usage_minimum,
       ROUND(AVG(z_score_usage_q1),2) AS z_score_usage_q1,
       ROUND(AVG(z_score_usage_median),2) AS z_score_usage_median,
       ROUND(AVG(z_score_usage_q3),2) AS z_score_usage_q3,
       ROUND(MAX(z_score_usage),2) AS z_score_usage_maximum
from (
SELECT account_id,
       prior_month_stats,
       prior_cent,
       churn_month,--prior 6 months (number)
       z_score_usage,
       plan_segment,
       months_before_churn,
      PERCENTILE_CONT(0.25) WITHIN GROUP
        (ORDER BY prior_month_stats) OVER (PARTITION BY months_before_churn) AS prior_months_stats_q1,
      MEDIAN(prior_month_stats) OVER (PARTITION BY months_before_churn) AS prior_months_stats_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP
        (ORDER BY prior_month_stats) OVER (PARTITION BY months_before_churn) AS prior_months_stats_q3,
      PERCENTILE_CONT(0.25) WITHIN GROUP
        (ORDER BY prior_cent) OVER (PARTITION BY months_before_churn) AS prior_cent_q1,
      MEDIAN(prior_cent) OVER (PARTITION BY months_before_churn) AS prior_cent_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP
        (ORDER BY prior_cent) OVER (PARTITION BY months_before_churn) AS prior_cent_q3,
      PERCENTILE_CONT(0.25) WITHIN GROUP
        (ORDER BY z_score_usage) OVER (PARTITION BY months_before_churn) AS z_score_usage_q1,
      MEDIAN(z_score_usage) OVER (PARTITION BY months_before_churn) AS z_score_usage_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP
        (ORDER BY z_score_usage) OVER (PARTITION BY months_before_churn) AS z_score_usage_q3
    FROM(
          SELECT m3.account_id,
                m1.churn_month,--prior 6 months (number)
                m1.prior_month_stats,
                m1.prior_cent,
                m1.z_score_usage,
                m3.plan_segment,
                date_diff('month', m3.churn_month, m1.prior_month) as months_before_churn
          FROM   ml_churn_data_preparation m1
          JOIN
              (SELECT m2.account_id, m2.churn_month, m2.plan_segment
              FROM ml_churn_data_preparation m2
              WHERE m2.churn_month between '2017-02-01' and '2018-10-31'
              and m2.churn='Yes' and m2.this_month_tenure > 3
              and (
                    (m2.plan_segment IN ('New Subscription') and m2.avg_invoice_three_months > 100)
                    or
                    (m2.plan_segment IN ('Utility Positive Invoice') and m2.avg_invoice_three_months > 500)
                    or
                    (m2.plan_segment IN ('Managed Contract', 'Non Managed Contract'))
                  ) -- plan segment filtering same as model
              ) m3
          ON  m1.account_id = m3.account_id
          and date_diff('month', m3.churn_month, m1.prior_month) between -6 and 0
          )
) group by 1,2
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: months_before_churn {
    type: string
    sql: ${TABLE}.months_before_churn ;;
  }

  dimension: plan_segment {
    type: string
    sql: ${TABLE}.plan_segment ;;
  }

  measure: prior_cent_minimum {
    type: min
    sql: ${TABLE}.prior_cent_minimum ;;
  }

  measure: prior_cent_q1 {
    type: max
    sql: ${TABLE}.prior_cent_q1 ;;
  }

  measure: prior_cent_median {
    type: median
    sql: ${TABLE}.prior_cent_median ;;
  }

  measure: prior_cent_q3 {
    type: max
    sql: ${TABLE}.prior_cent_q3 ;;
  }

  measure: prior_cent_maximum {
    type: max
    sql: ${TABLE}.prior_cent_maximum ;;
  }

  measure: prior_month_stats_minimum {
    type: min
    sql: ${TABLE}.prior_month_stats_minimum ;;
  }

  measure: prior_months_stats_q1 {
    type: max
    sql: ${TABLE}.prior_months_stats_q1 ;;
  }

  measure: prior_month_stats_median {
    type: median
    sql: ${TABLE}.prior_month_stats_median ;;
  }

  measure: prior_months_stats_q3 {
    type: max
    sql: ${TABLE}.prior_months_stats_q3 ;;
  }

  measure: prior_month_stats_maximum {
    type: max
    sql: ${TABLE}.prior_month_stats_maximum ;;
  }

  measure: z_score_usage_minimum {
    type: min
    sql: ${TABLE}.z_score_usage_minimum ;;
  }

  measure: z_score_usage_q1 {
    type: max
    sql: ${TABLE}.z_score_usage_q1 ;;
  }

  measure: z_score_usage_median {
    type: median
    sql: ${TABLE}.z_score_usage_median ;;
  }

  measure: z_score_usage_q3 {
    type: max
    sql: ${TABLE}.z_score_usage_q3 ;;
  }

  measure: z_score_usage_maximum {
    type: max
    sql: ${TABLE}.z_score_usage_maximum ;;
  }

  set: detail {
    fields: [
      months_before_churn,
      prior_cent_minimum,
      prior_cent_q1,
      prior_cent_median,
      prior_cent_q3,
      prior_cent_maximum,
      prior_month_stats_minimum,
      prior_months_stats_q1,
      prior_month_stats_median,
      prior_months_stats_q3,
      prior_month_stats_maximum
    ]
  }
}
