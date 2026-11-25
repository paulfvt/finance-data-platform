{#
    Échoue s'il existe plusieurs lignes pour une même combinaison de colonnes
    (ex. un actif présent deux fois le même jour). Équivalent maison de
    dbt_utils.unique_combination_of_columns, pour ne pas dépendre d'un paquet.
#}
{% test unique_combination(model, columns) %}

select {{ columns | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
