-- Estimated gambling tax: a configurable share (var gambling_tax_rate) of the gambling margin
-- (stakes minus winnings). A negative month gives no tax; it is not carried forward here.
-- Illustrative only, not tax advice.
with monthly as (
    select
        date_trunc('month', business_date) as tax_month,
        product,
        sum(turnover_eur) as turnover_eur,
        sum(winnings_eur) as winnings_eur,
        sum(ggr_eur) as ggr_eur
    from {{ ref('fct_ggr_daily') }}
    group by 1, 2
)

select
    cast(tax_month as date) as tax_month,
    product,
    {{ money('turnover_eur') }} as turnover_eur,
    {{ money('winnings_eur') }} as winnings_eur,
    {{ money('ggr_eur') }} as ggr_eur,
    {{ money('greatest(ggr_eur, 0)') }} as tax_base_eur,
    {{ var('gambling_tax_rate') }} as tax_rate,
    {{ money("round(greatest(ggr_eur, 0) * " ~ var('gambling_tax_rate') ~ ", 2)") }} as tax_eur
from monthly
