{# build a list of models looping through all models in the project #}
{# filter by directory or prefix arguments, if provided #}
{% macro get_model_names(directory=None, prefix=None) %}
    {% set model_names=[] %}
    {% if execute %}
    {% set models = graph.nodes.values() | selectattr('resource_type', "equalto", 'model') %}
    {% endif %}
    log(str(models), info=True)
    {% if directory and prefix %}
        {% for model in models %}
            {% set model_path = "/".join(model.path.split("/")[:-1]) %}
            {% if model_path == directory and model.name.startswith(prefix) %}
                {% do model_names.append(model.name) %}
            {% endif %} 
        {% endfor %}
    {% elif directory %}
        {% for model in models %}
            {% set model_path = "/".join(model.path.split("/")[:-1]) %}
            {% if model_path == directory %}
                {% do model_names.append(model.name) %}
            {% endif %}
        {% endfor %}
    {% elif prefix %}

        {% for model in models if model.name.startswith(prefix) %} #}
            {% do model_names.append(model.name) %}
        {% endfor %}
    {% else %}
        {% for model in models %}
            {% do model_names.append(model.name) %}
        {% endfor %}
    {% endif %}
    {{ return(model_names) }}
{% endmacro %}