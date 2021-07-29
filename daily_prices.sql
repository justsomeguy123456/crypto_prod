

select p.id,p.sym,p.price,p.name, a.date
from (
select id,sym,link,name,cast(date_time as date) date ,max(date_time) date_time
from prices
group by id,sym,link,name,cast(date_time as date)
	
) a
join prices p on p.id = a.id and p.date_time = a.date_time
--and p.sym = 'hnt'
order by cast(p.date_time as date)




