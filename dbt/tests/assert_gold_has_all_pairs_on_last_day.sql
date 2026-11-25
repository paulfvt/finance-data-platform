-- Le dernier jour de Gold doit contenir toutes les paires d'actifs : n x (n - 1) / 2.
-- Une paire manquante signale un actif sans assez d'historique commun, qui
-- disparaîtrait sans bruit de la heatmap des corrélations.

with last_day as (
    select max(price_date) as price_date from {{ ref('gold_correlations_30d') }}
),

expected as (
    select count(*) * (count(*) - 1) / 2 as n_pairs from {{ ref('tickers') }}
),

actual as (
    select count(*) as n_pairs
    from {{ ref('gold_correlations_30d') }}
    where price_date = (select price_date from last_day)
)

select expected.n_pairs as paires_attendues, actual.n_pairs as paires_presentes
from expected, actual
where actual.n_pairs <> expected.n_pairs
