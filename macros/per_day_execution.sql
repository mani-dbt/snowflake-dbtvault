{% macro per_day_execution(paramvalue) %}

    {% for i in range(paramvalue) %}

        {{ in_day_execution(i) }}
        {{ log(i, info=True) }}

    {% endfor %}

{% endmacro %}


