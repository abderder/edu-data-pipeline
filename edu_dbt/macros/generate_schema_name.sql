{% macro generate_schema_name(custom_schema_name, node) %}
    {% if custom_schema_name is not none %}
        {{ custom_schema_name | trim }}
    {% elif 'gold' in node.path %}
        GOLD
    {% elif 'silver' in node.path %}
        SILVER
    {% elif 'bronze' in node.path %}
        BRONZE
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}
