
select 
a.symbol,
sum(a.qty::NUMERIC) qty,
sum(a.basis::NUMERIC) *-1 basis ,
p.price ::NUMERIC price,
p.price * sum(a.qty::NUMERIC) as  value,
(sum(a.basis::NUMERIC) - (p.price::NUMERIC * sum(a.qty::NUMERIC)))*-1 g_l
,case when avg_price.rolling_qty < -0.02 then 0 else avg_price.rolling_qty end rolling_qty ,
avg_price.rolling_basis,
avg_price.avg_price,
avg_price.realized_gain_total *-1 realized_gain_total
from(
select bl.symbol, sum(bl.qty) qty, sum(bl."quoteQty") basis--, /*, bl.time ,*/ 'b'
from public.binance_ledger bl
--where symbol = 'DOGE'
group by bl.symbol
union 
select cl.coin, sum(cl.amt), sum(cl.basis)--,/*, cl.created_at,*/ 'cb'
from coinbase_ledger cl
--where coin = 'DOGE'
group by cl.coin
union
select wallet.coin, case when wallet.coin = 'BNB' then wallet.amt::NUMERIC + 1 else wallet.amt::NUMERIC end ,0 
	from wallet  
	join (
	select max(date_added) date, coin, address
	from wallet
		group by  coin, address
	) b on b.coin = wallet.coin and b.date = wallet.date_added

	) a
	 join public.prices p on upper(p.sym) = case when a.symbol = 'ETH2' then 'ETH' else a.symbol end and p.date_time = 
(
select max(p1.date_time)
from public.prices p1 
)join (


select avg_prices_ledger.symbol ,
case when avg_prices_ledger.rolling_qty < -0.02 then 0 else avg_prices_ledger.rolling_qty end rolling_qty,
avg_prices_ledger.rolling_basis,
avg_prices_ledger.avg_price,
avg_prices_ledger.realized_gain_total *-1 realized_gain_total,
	a.time
	,avg_prices_ledger."isBuyer"
from avg_prices_ledger
join (
select max(time) as time, symbol
	from avg_prices_ledger
	group by symbol

) a on  a.symbol  = avg_prices_ledger.symbol and a.time = avg_prices_ledger.time
 join(
select max(time) as time, symbol	,avg_prices_ledger."isBuyer"
	,case when avg_prices_ledger."isBuyer" = 'Sell' then Min(rolling_qty) else max(rolling_qty) end rolling_qty

	from avg_prices_ledger
	--where symbol = 'NANO'
	group by symbol,avg_prices_ledger."isBuyer"

) b on b.symbol  = avg_prices_ledger.symbol and b.time = avg_prices_ledger.time 
	and b.rolling_qty = case when avg_prices_ledger.rolling_qty < -0.02 then 0 else avg_prices_ledger.rolling_qty end
--where avg_prices_ledger.symbol = 'NANO'
	
	
	
	
	
) avg_price on avg_price.symbol = a.symbol
where a.symbol is not null
	group by  a.symbol, p.price,avg_price.rolling_qty,
avg_price.rolling_basis,
avg_price.avg_price,
avg_price.realized_gain_total *-1 
--having sum(a.qty::NUMERIC) > 0
	order by 1
	
	
	
