-- Gold : corrélation glissante sur 30 jours entre chaque paire d'actifs, format long
-- (une ligne par jour et par paire).
--
-- Les marchés traditionnels ferment le week-end, la crypto cote tous les jours :
-- les séries sont alignées sur un calendrier complet avant le calcul.
--
-- ATTENTION, reproduit à l'identique de l'ancien code pandas : les jours sans
-- cotation reprennent le dernier RENDEMENT connu (ffill). Le mouvement du
-- vendredi compte donc trois fois pour les actions. Biais corrigé dans un
-- commit séparé, pour que la migration reste vérifiable.

{{ config(
    materialized = 'external',
    location = env_var('DATA_DIR', '/opt/airflow/data') ~ '/dbt/gold_correlations_30d.parquet',
    format = 'parquet'
) }}

{% set window_days = 30 %}

with silver as (

    select ticker_name, price_date, daily_return
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

aligned as (

    -- Calendrier complet x actifs, rendement manquant complété par le dernier connu
    select
        c.price_date,
        a.ticker_name,
        a.display_order,
        last_value(s.daily_return ignore nulls) over (
            partition by a.ticker_name
            order by c.price_date
            rows between unbounded preceding and current row
        ) as daily_return
    from calendar c
    cross join assets a
    left join silver s
        on s.ticker_name = a.ticker_name
       and s.price_date = c.price_date

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
