{% macro rolling_average_x_periods(column_name, partition_by, number_of_periods, order_by='created_at') %}
    avg( {{ column_name }} ) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ number_of_periods }} PRECEDING AND CURRENT ROW
            ) AS avg_{{number_of_periods}}_periods_{{ column_name }}
{% endmacro %}
