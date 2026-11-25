-- Une ligne par actif et par jour de cotation, dans sa version la plus récente.
--
-- Un même jour est relu par plusieurs runs Bronze (fenêtre glissante). On garde
-- celui du run le plus récent : pour la crypto, la bougie du jour est encore
-- incomplète lors des premiers runs (même règle que keep="last" dans l'ancien
-- code pandas).

with source as (

    select
        cast(date as date)                                                  as price_date,
        regexp_extract(filename, '([^/\\]+)\.parquet$', 1)                  as ticker_name,
        ticker,
        open,
        high,
        low,
        close,
        volume,
        cast(regexp_extract(filename, '(\d{4}-\d{2}-\d{2})', 1) as date)     as run_date
    from {{ source('bronze', 'prices') }}

),

latest_version as (

    select *
    from source
    qualify row_number() over (
        partition by ticker_name, price_date
        order by run_date desc
    ) = 1

)

select * from latest_version
