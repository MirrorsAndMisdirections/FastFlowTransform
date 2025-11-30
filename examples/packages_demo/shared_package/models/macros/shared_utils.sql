{# Shared SQL macros for the package #}

{%- macro email_domain(expr) -%}
  -- Extract domain part from an email address in a simple, portable way.
  lower(regexp_replace({{ expr }}, '^.*@', ''))
{%- endmacro -%}
