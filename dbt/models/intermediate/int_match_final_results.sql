-- The highest result_version is authoritative; earlier versions were corrected.
select
    match_id,
    outcome,
    result_version,
    resulted_at_utc,
    result_version > 1 as was_corrected
from {{ ref('stg_match_results') }}
qualify row_number() over (partition by match_id order by result_version desc) = 1
