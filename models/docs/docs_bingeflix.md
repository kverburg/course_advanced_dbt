# Bingeflix Docs
This file contains doumentation for Bingeflix data sources.

## Users
This section contains documentation from the Bingeflix Users table.
{% docs source_users %}
This table stores information about Bingeflix users, including user IDs, usernames, email addresses, subscription start dates, and other relevant user details
{% enddocs %}

{% docs bingeflix_column_user_id %}
The unique identifier of the Bingeflix user. A user is created when...
{% enddocs %}

## Subscriptions 

{% docs source_subscriptions %}
This table contains subscription-related information, such as subscription plan IDs, subscription start dates, renewal dates, billing information, and any other relevant subscription details. 
{% enddocs %}

## Subscription plans

{% docs source_subscription_plans %}
This table contains information about the subscription plans available at Bingeflix.
{% enddocs %}

## Events

{% docs source_events %}
This table contains information about the behavioral events of users while they interact with the Bingeflix platform. It includes events such as logins, logouts, videos watched, and CTA/button clicks.
{% enddocs %}

## Ads

{% docs source_ads %}
This table contains information about Bingeflix's marketing ads.
{% enddocs %}