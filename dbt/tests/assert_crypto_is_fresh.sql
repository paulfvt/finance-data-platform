-- Fraîcheur des DONNÉES (pas seulement des fichiers) : la crypto cote tous les jours,
-- donc sa dernière cotation doit dater d'hier au plus tard. Sinon l'extraction a
-- tourné sans rien rapporter de neuf (API muette, ticker renommé...).
-- Renvoie une ligne par crypto en retard : le test échoue s'il y en a.

select
    ticker_name,
    max(price_date)                             as derniere_cotation,
    current_date - max(price_date)              as jours_de_retard
from {{ ref('stg_bronze_prices') }}
where ticker_name in (
    select ticker_name from {{ ref('tickers') }} where asset_class = 'crypto'
)
group by ticker_name
having max(price_date) < current_date - 1
