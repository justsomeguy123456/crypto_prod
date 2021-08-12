

select * from crypto_portfolio_historical


delete from crypto_portfolio_historical where asofdate not in(

select asofdate from(

select max(asofdate) asofdate, cast(asofdate as date)  date
from crypto_portfolio_historical
group by cast(asofdate as date) 
) a
	)
