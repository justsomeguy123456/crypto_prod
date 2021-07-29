

select main.date, sum(rolling_basis) basis, sum(main.value) val

from (
select ch.symbol, ch.rolling_qty, ch.rolling_basis, a.date, p.price, (p.price * ch.rolling_qty)as value
from(
select symbol, cast(asofdate as date) date, max(asofdate) asofdate
from crypto_portfolio_historical
group by cast(asofdate as date),symbol
) a
join crypto_portfolio_historical ch on ch.asofdate = a.asofdate and ch.symbol = a.symbol
 join (

select p.id,upper(p.sym) sym,p.price,p.name, a.date
from (
select id,sym,link,name,cast(date_time as date) date ,max(date_time) date_time
from prices
group by id,sym,link,name,cast(date_time as date)
	
) a
join prices p on p.id = a.id and p.date_time = a.date_time

) p on p.sym = a.symbol and p.date = a.date
	)main
	group by main.date
order by main.date