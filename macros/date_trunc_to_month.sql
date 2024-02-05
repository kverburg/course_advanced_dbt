{% macro date_trunc_to_month(column_name) %}

    DATE(DATE_TRUNC('month', {{ column_name }} ))

{% endmacro %}