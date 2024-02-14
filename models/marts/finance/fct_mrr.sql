{{ config(tags="p0") }}
{% set import_subscriptions = unit_testing_select_table(ref('dim_subscriptions'), ref('dim_subscriptions_seed')) %}
{% set import_dates = unit_testing_select_table(ref('int_dates'), ref('dim_date_seed')) %}

-- This model is created following the dbt MRR playbook: https://www.getdbt.com/blog/modeling-subscription-revenue/

WITH

subscription_revenue_by_month AS (
    SELECT 
        *
    FROM 
        {{ ref('int_subscription_revenue_by_month') }}
),

subscription_periods AS (
    SELECT 
        *
    FROM 
        {{ ref('int_subscription_periods')}}
),

-- Calculate subscriber level churn by month by getting row for month *after* last month of activity
subscription_churn_by_month AS (
    SELECT
        DATEADD(MONTH, 1, date_month)::DATE AS date_month,
        user_id,
        subscription_id,
        FALSE AS is_subscribed_current_month,
        first_subscription_month,
        last_subscription_month,
        FALSE AS is_first_subscription_month,
        FALSE AS is_last_subscription_month,
        0.0::DECIMAL(18, 2) AS mrr
    FROM
        subscription_revenue_by_month
    WHERE
        is_last_subscription_month = TRUE
),

-- Union monthly revenue and churn CTEs
unioned AS (
    SELECT * FROM subscription_revenue_by_month
    UNION ALL
    SELECT * FROM subscription_churn_by_month
),

-- Get prior month MRR and calculate MRR change
mrr_with_changes AS (
    SELECT
        *,

        COALESCE(
            LAG(is_subscribed_current_month) OVER (PARTITION BY user_id, subscription_id ORDER BY date_month),
            FALSE
        ) AS is_subscribed_previous_month,

        COALESCE(
            LAG(mrr) OVER (PARTITION BY user_id, subscription_id ORDER BY date_month),
            0.0
        ) AS previous_month_mrr_amount,

        mrr - previous_month_mrr_amount AS mrr_change
    FROM
        unioned
),

-- Add surrogate key and classify months as new, churn, reactivation, upgrade, downgrade, or renewal
final AS (
    SELECT
        mrr_with_changes.date_month,
        mrr_with_changes.user_id,
        mrr_with_changes.subscription_id,
        subscription_periods.starts_at,
        subscription_periods.ends_at,
        subscription_periods.plan_name,
        mrr AS mrr_amount,
        mrr_change,
        LEAST(mrr, previous_month_mrr_amount) AS retained_mrr_amount,
        previous_month_mrr_amount,

        CASE
            WHEN is_first_subscription_month THEN 'new'
            WHEN NOT(is_subscribed_current_month) AND is_subscribed_previous_month THEN 'churn'
            WHEN is_subscribed_current_month AND NOT(is_subscribed_previous_month) THEN 'reactivation'
            WHEN mrr_change > 0.0 THEN 'upgrade'
            WHEN mrr_change < 0.0 THEN 'downgrade'
            ELSE 'renewal'
        END AS change_category,

        -- Add month_retained_number for cohort analysis
        CASE
            WHEN change_category = 'churn' THEN NULL
            ELSE DATEDIFF('month', first_subscription_month, date_month)
        END AS month_retained_number

    FROM
        mrr_with_changes
        LEFT JOIN subscription_periods
            ON mrr_with_changes.user_id = subscription_periods.user_id
                AND mrr_with_changes.subscription_id = subscription_periods.subscription_id
    WHERE
        date_month <= {{ date_trunc_to_month('CURRENT_DATE') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_month', 'subscription_id', 'change_category']) }} AS surrogate_key,
    *
FROM final
