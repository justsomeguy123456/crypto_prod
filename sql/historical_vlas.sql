
	
	
	--delete from crypto_portfolio_historical where symbol = 'DOGE' and "quoteQty" in (-34.697, -30.4109) and "isBuyer" = 'Sell'
	
select 
sum(rolling_basis)basis,
sum(val) val,
sum(realized_gain_total) g_l,
date
from (
select ch.rolling_basis,
ch.symbol, 
ch.asofdate,
ch.rolling_qty,
ch.rolling_qty * b.price val,
	ch.realized_gain_total,
a.date
from crypto_portfolio_historical ch
join (

SELECT Max(ch2.asofdate)      asofdate,
Cast(asofdate AS DATE) date,
ch2.symbol
FROM   crypto_portfolio_historical ch2
--    WHERE  symbol = 'DOGE'
GROUP  BY Cast(asofdate AS DATE),
ch2.symbol
order by 1
) a on a.asofdate = ch.asofdate and a.symbol = ch.symbol
join (

select cast(p.date_time as date) date,
p.date_time,
p.name, 
upper(p.sym) sym,
p.price
from prices p join(			  
select max(p.date_time) date_time, sym, cast(p.date_time as date) "date"
from prices p
group by sym, cast(p.date_time as date)
) price1 on price1.sym = p.sym and price1.date_time = p.date_time
	) b on b.sym = ch.symbol and a.date = b.date
)main
group by date