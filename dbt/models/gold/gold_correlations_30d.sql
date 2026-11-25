-- Gold : corrélation glissante sur 30 jours entre chaque paire d'actifs, format long
-- (une ligne par jour et par paire).
--
-- Les marchés traditionnels ferment le week-end, la crypto cote tous les jours :
-- les séries sont alignées sur un calendrier complet avant le calcul.
--
-- Les jours sans cotation reprennent le dernier PRIX connu, et les rendements
-- sont calculés ensuite sur le calendrier complet : un samedi donne ln(P/P) = 0
-- (pas d'information), le lundi porte tout le mouvement depuis vendredi, et
-- chaque mouvement compte une seule fois. (L'ancien code recopiait le dernier
-- RENDEMENT : le mouvement du vendredi comptait trois fois.)

{{ config(
    materialized = 'external',
    location = env_var('DATA_DIR', '/opt/airflow/data') ~ '/gold/correlations_30d.parquet',
    format = 'parquet'
) }}

{# Variable plutôt que constante : les tests unitaires la réduisent à 3 jours #}
{% set window_days = var('correlation_window_days', 30) %}
{# Plus longue fermeture normale d'un marché (pont de 4 jours). Au-delà, c'est un
   trou de DONNÉES (extraction arrêtée), pas un week-end : on ne complète pas. #}
{% set max_fill_days = 4 %}

with silver as (

    select ticker_name, price_date, close
    from {{ ref('silver_daily_prices') }}

),

calendar as (

    select cast(day as date) as price_date
    from generate_series(
        (select min(price_date) from silver),
        (select max(price_date) from silver),
        interval 1 day
    ) as t(day)

),

assets as (

    select ticker_name, display_order
    from {{ ref('tickers') }}
    where ticker_name in (select distinct ticker_name from silver)

),

last_known as (

    -- Calendrier complet x actifs, avec le dernier prix connu et sa date
    select
        c.price_date,
        a.ticker_name,
        a.display_order,
        last_value(s.close ignore nulls) over running        as last_close,
        last_value(s.price_date ignore nulls) over running   as last_quote_date
    from calendar c
    cross join assets a
    left join silver s
        on s.ticker_name = a.ticker_name
       and s.price_date = c.price_date
    window running as (
        partition by a.ticker_name
        order by c.price_date
        rows between unbounded preceding and current row
    )

),

filled_prices as (

    -- Prix complété seulement sur une fermeture normale ({{ max_fill_days }} jours au plus).
    -- Après un trou plus long, le prix reste inconnu : pas de rendement inventé, et
    -- pas de corrélation tant que la fenêtre ne contient pas assez de jours réels.
    select
        price_date,
        ticker_name,
        display_order,
        case
            when price_date - last_quote_date <= {{ max_fill_days }} then last_close
        end as close
    from last_known

),

aligned as (

    -- Rendement d'un jour calendaire au suivant : 0 quand le marché était fermé
    select
        price_date,
        ticker_name,
        display_order,
        ln(close / lag(close) over (partition by ticker_name order by price_date)) as daily_return
    from filled_prices

),

pairs as (

    select
        x.price_date,
        x.ticker_name   as asset_1,
        y.ticker_name   as asset_2,
        x.daily_return  as return_1,
        y.daily_return  as return_2
    from aligned x
    join aligned y
        on y.price_date = x.price_date
       and y.display_order > x.display_order

),

rolling as (

    select
        price_date,
        asset_1,
        asset_2,
        corr(return_1, return_2) over last_window                                         as correlation_30d,
        count(case when return_1 is not null and return_2 is not null then 1 end) over last_window as n_pairs
    from pairs
    window last_window as (
        partition by asset_1, asset_2
        order by price_date
        rows between {{ window_days - 1 }} preceding and current row
    )

)

-- Comme pandas (min_periods = fenêtre) : 30 jours où les deux rendements sont connus
select price_date, asset_1, asset_2, correlation_30d
from rolling
where n_pairs = {{ window_days }}
  and isfinite(correlation_30d)
order by asset_1, asset_2, price_date
