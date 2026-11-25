-- Complétude : chaque actif du référentiel doit avoir une cotation de moins de
-- 7 jours (le plus long pont boursier dure 4 jours). Un actif absent ou figé
-- signale une extraction cassée pour lui seul, que les autres masqueraient.

select
    t.ticker_name,
    max(s.price_date) as derniere_cotation
from {{ ref('tickers') }} t
left join {{ ref('stg_bronze_prices') }} s
    on s.ticker_name = t.ticker_name
group by t.ticker_name
having max(s.price_date) is null
    or max(s.price_date) < current_date - 7
