


delete from prices where date_time not in (
	
	
select date_time 
from (
select max(date_time) date_time, cast(date_time as date) as "Date"
from prices
group by cast(date_time as date)
) a
	
	
	)
	
	
	
	
	