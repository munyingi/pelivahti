select * from {{ ref('fct_lottery_tax_monthly') }}
where tax_eur <> round(greatest(ggr_eur, 0) * {{ var('lottery_tax_rate') }}, 2)
