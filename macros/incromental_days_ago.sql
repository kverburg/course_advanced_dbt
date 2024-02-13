{% macro incremental_days_ago(table_name, column_name, days_ago=1) %}

WHERE datediff('day',{{ column_name }} , (SELECT MAX( {{ column_name }} ) FROM {{ table_name }} )) >= {{ days_ago }}

{% endmacro %}
