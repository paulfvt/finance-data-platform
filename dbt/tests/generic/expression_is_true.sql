{#
    Échoue pour chaque ligne où l'expression SQL est fausse
    (ex. "high >= low", "close > 0"). Une expression NULL compte comme fausse,
    sauf si `where` exclut ces lignes.
#}
{% test expression_is_true(model, expression, where=None) %}

select *
from {{ model }}
where not coalesce(({{ expression }}), false)
{% if where %}
  and ({{ where }})
{% endif %}

{% endtest %}
