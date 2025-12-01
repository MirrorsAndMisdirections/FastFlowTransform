{# Macros for the git-based package #}

{%- macro git_email_domain(expr) -%}
  -- Slightly different macro than the local package
  lower(regexp_replace({{ expr }}, '^.*@', ''))
{%- endmacro -%}
