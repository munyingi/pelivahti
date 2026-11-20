-- One STAKE per bet or round, whatever was redelivered.
select ref_id, count(*) as n
from {{ source('realtime', 'ledger') }}
where entry_type = 'STAKE'
group by ref_id
having count(*) > 1
