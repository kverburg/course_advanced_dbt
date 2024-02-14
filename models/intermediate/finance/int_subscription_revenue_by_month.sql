with

subscription_periods as (
    select * 
    from {{ ref('int_subscription_periods') }}
),

months as (
    select 
        *
    from 
        {{ ref('int_dates') }} 
),

subscribers as (
    select
        user_id,
        subscription_id,
        MIN(start_month) as first_start_month,
        MAX(end_month) as last_end_month
    from
        subscription_periods
    group by
        1, 2
),

-- Create one record per month between a subscriber's first and last month
subscriber_months as (
    select
        subscribers.user_id,
        subscribers.subscription_id,
        months.date_month
    from
        subscribers
        inner join months
            -- All months after start date
            on months.date_month >= subscribers.first_start_month
                -- and before end date
                and subscribers.last_end_month > months.date_month
),

-- Join together to create base CTE for MRR calculations
mrr_base as (
    select
        subscriber_months.date_month,
        subscriber_months.user_id,
        subscriber_months.subscription_id,
        COALESCE(subscription_periods.monthly_amount, 0.0) as mrr
    from
        subscriber_months
        left join subscription_periods
            on subscriber_months.user_id = subscription_periods.user_id
                and subscriber_months.subscription_id = subscription_periods.subscription_id
                -- The month is on or after the subscription start date...
                and subscriber_months.date_month >= subscription_periods.start_month
                -- and the month is before the subscription end date (and handle NULL case)
                and (subscriber_months.date_month < subscription_periods.end_month
                    or subscription_periods.end_month is NULL)
),

-- Calculate subscriber level MRR (monthly recurring revenue)
subscription_revenue_by_month as (
    select
        date_month,
        user_id,
        subscription_id,
        mrr > 0 as is_subscribed_current_month,

        -- Find the subscriber's first month and last subscription month
        MIN(case when is_subscribed_current_month then date_month end) over (partition by user_id, subscription_id) as first_subscription_month,
        MAX(case when is_subscribed_current_month then date_month end) over (partition by user_id, subscription_id) as last_subscription_month,
        first_subscription_month = date_month as is_first_subscription_month,
        last_subscription_month = date_month as is_last_subscription_month,
        mrr
    from
        mrr_base
)

select * from subscription_revenue_by_month