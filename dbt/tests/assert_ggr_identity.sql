select * from {{ ref('fct_ggr_daily') }}
where ggr_eur <> turnover_eur - winnings_eur
