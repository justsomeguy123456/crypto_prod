




delete from wallet where cast(date_added as varchar)||address not in (



select cmb from (


select max(date_added) date_added, coin, address, cast(date_added as date) as "date", cast(max(date_added) as varchar)||address as cmb
from wallet
where coin = 'LINK'
group by coin, cast(date_added as date), address

) a
) and coin = 'LINK'

