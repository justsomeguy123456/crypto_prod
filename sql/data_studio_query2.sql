

select avg1.symbol,
case when avg1.rolling_qty::NUMERIC < .00000000001 then 0::numeric else  avg1.rolling_qty::NUMERIC end as qty,
avg1.rolling_basis::numeric *-1 + avg1.realized_gain_total *-1  as basis,
price_data.price::numeric as price,
price_data.price::NUMERIC * avg1.rolling_qty::numeric as value,
(avg1.rolling_basis - price_data.price::NUMERIC * avg1.rolling_qty::numeric)*-1 as g_l,
case when avg1.rolling_qty::NUMERIC < .00000000001 then 0::numeric else  avg1.rolling_qty::NUMERIC end  rolling_qty,
avg1.rolling_basis,
avg1.avg_price,
avg1.realized_gain_total *-1 as realized_gain_total
from(

select * from avg_prices_ledger a 
join (

select max(index) as  index, symbol as symbol1
from avg_prices_ledger
group by symbol
	) b on b.index = a.index and b.symbol1 = a.symbol
) avg1
join(
	select * from prices p where p.date_time =
(select max(p1.date_time) 
from prices p1 
)
) price_data on upper(price_data.sym) = upper(avg1.symbol)
