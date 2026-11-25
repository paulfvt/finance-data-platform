-- Chaque paire de Gold doit être orientée dans l'ordre du référentiel :
-- (sp500, bitcoin) et jamais (bitcoin, sp500), ni (sp500, sp500).
-- Sinon la même paire pourrait apparaître sous deux noms dans le dashboard.

select g.asset_1, g.asset_2, count(*) as lignes
from {{ ref('gold_correlations_30d') }} g
join {{ ref('tickers') }} t1 on t1.ticker_name = g.asset_1
join {{ ref('tickers') }} t2 on t2.ticker_name = g.asset_2
where t1.display_order >= t2.display_order
group by g.asset_1, g.asset_2
