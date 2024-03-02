{# depends_on: {{ ref('stg__yellow_tripdata') }}
depends_on: {{ ref('stg__green_tripdata') }} #}

{% set models_to_generate = get_model_names(prefix='stg_') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}
