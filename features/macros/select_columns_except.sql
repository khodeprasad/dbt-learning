{% macro select_columns_except(source_table, exclude_columns) %}
    {% set columns = adapter.get_columns_in_relation(ref(source_table)) %}
    {% set include_columns = [] %}
    {% for column in columns %}
        {% if column.name not in exclude_columns %}
            {% do include_columns.append(column.name) %}
        {% endif %}
    {% endfor %}
    {{ include_columns | join(', ') }}
{% endmacro %}
