{# Select * except some columns. Works across engines. #}
{%- macro star_except(relation, exclude_cols) -%}
{%- set excl = exclude_cols | map('lower') | list -%}
{%- set cols = adapter_columns(relation) -%}
{# adapter_columns is provided by FFT executors' catalog/describe (if available).
   To keep demo simple, fall back to literal star if unknown. #}
{%- if cols and cols|length > 0 -%}
    {{- (cols | reject('in', excl) | map('string') | join(', ')) -}}
{%- else -%}
    *
{%- endif -%}
{%- endmacro -%}
