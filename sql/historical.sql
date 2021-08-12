select sum(f.rolling_basis) basis,
	f.date,
	   sum(f.value) val
	   
from (
SELECT DISTINCT a.symbol,
                a.date,
                a.rolling_qty,
                d.price,
				a.rolling_qty * d.price "value",
                c.rolling_basis,
                a.asofdate
FROM  (SELECT DISTINCT symbol,
                       Cast(asofdate AS DATE) date,
                       Max(asofdate)          asofdate,
                       "isBuyer",
                       CASE
                         WHEN "isBuyer" = 'Sell' THEN (SELECT Min(rolling_qty)
                                                       FROM
                         crypto_portfolio_historical ch
                                                       WHERE
                         ch.symbol = ch1.symbol
                         AND ch.asofdate =
                             ch1.asofdate)
                         ELSE (SELECT Max(rolling_qty)
                               FROM   crypto_portfolio_historical ch
                               WHERE  ch.symbol = ch1.symbol
                                      AND ch.asofdate = ch1.asofdate)
                       END                    rolling_qty
       FROM   crypto_portfolio_historical ch1
      -- WHERE  symbol = 'DOGE'
       GROUP  BY symbol,
                 asofdate,
                 "isBuyer") a
      JOIN (SELECT Max(ch2.asofdate)      asofdate,
                   Cast(asofdate AS DATE) date,
                   ch2.symbol
            FROM   crypto_portfolio_historical ch2
        --    WHERE  symbol = 'DOGE'
            GROUP  BY Cast(asofdate AS DATE),
                      ch2.symbol) b
        ON b.symbol = a.symbol
           AND b.asofdate = a.asofdate
      JOIN (SELECT ch3.rolling_basis,
                   ch3.symbol,
                   ch3.asofdate,
                   ch3.rolling_qty
            FROM   crypto_portfolio_historical ch3) c
        ON c.asofdate = a.asofdate
           AND c.symbol = a.symbol
           AND c.rolling_qty = a.rolling_qty
      JOIN (SELECT id,
                   sym,
                   link,
                   NAME,
                   Cast(date_time AS DATE) date,
                   Max(date_time)          date_time
            FROM   prices
          --  WHERE  sym = 'doge'
            GROUP  BY id,
                      sym,
                      link,
                      NAME,
                      Cast(date_time AS DATE)) price
        ON Upper(price.sym) = a.symbol
           AND price.date = a.date
      JOIN (SELECT p.sym,
                   p.date_time,
                   p.price
            FROM   prices p) d
        ON d.sym = price.sym
           AND d.date_time = price.date_time
	)f
	group by f.date
ORDER  BY f.date