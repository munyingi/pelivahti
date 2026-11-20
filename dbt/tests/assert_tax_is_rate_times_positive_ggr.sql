select * from {{ ref('fct_gambling_tax_monthly') }}
where tax_eur <> round(greatest(ggr_eur, 0) * {{ var('gambling_tax_rate') }}, 2)
