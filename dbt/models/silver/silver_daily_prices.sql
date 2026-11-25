-- Silver : historique quotidien nettoyé et enrichi, une ligne par actif et par jour de cotation.
--
-- Fenêtres en LIGNES (jours de cotation), pas en temps : une moyenne mobile 20 jours
-- porte sur les 20 dernières séances, week-ends et jours fériés exclus. C'est la
-- convention des marchés, et l'inverse de la couche live, où les fenêtres sont en
-- temps parce que le flux peut avoir des trous.

{{ config(
    materialized = 'external',
    location = env_var('DATA_DIR', '/opt/airflow/data') ~ '/dbt/silver_daily_prices.parquet',
    format = 'parquet'
) }}

with prices as (

    -- Prix de clôture manquant ou aberrant : la ligne est écartée (comme avant en pandas)
    select *
    from {{ ref('stg_bronze_prices') }}
    where close is not null
      and close > 0

),

returns as (

    select
        *,
        close / lag(close) over by_day as price_ratio
    from prices
    window by_day as (partition by ticker_name order by price_date)

),

metrics as (

    select
        ticker_name,
        ticker,
        price_date,
        open,
        high,
        low,
        close,
        volume,
        -- Rendement log ; un ratio nul ou négatif donne NULL plutôt qu'une erreur
        case when price_ratio > 0 then ln(price_ratio) end                    as daily_return,
        avg(close) over last_20                                               as ma_20,
        avg(close) over last_50                                               as ma_50
    from returns
    window
        by_day  as (partition by ticker_name order by price_date),
        last_20 as (partition by ticker_name order by price_date rows between 19 preceding and current row),
        last_50 as (partition by ticker_name order by price_date rows between 49 preceding and current row)

)

select
    *,
    -- Volatilité : écart-type des rendements sur 20 séances, à partir de 2 rendements connus
    case
        when count(daily_return) over last_20 >= 2
        then stddev_samp(daily_return) over last_20
    end                                                                       as volatility_20d
from metrics
window last_20 as (partition by ticker_name order by price_date rows between 19 preceding and current row)
order by ticker_name, price_date
