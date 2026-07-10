CREATE SCHEMA nasdaq;

CREATE AGGREGATE public.mul(numeric) (
  SFUNC=numeric_mul,
  STYPE=numeric,
  INITCOND='1'
);

CREATE OR REPLACE FUNCTION public.closest_step(
  current_best anyelement,
  next_any anyelement,
  target_any anyelement
)
RETURNS anyelement AS $$
BEGIN
  IF current_best IS NULL THEN RETURN next_any; END IF;
  IF next_any IS NULL THEN RETURN current_best; END IF;

  IF ABS(next_any - target_any) < ABS(current_best - target_any) THEN
    RETURN next_any;
  ELSE
    RETURN current_best;
  END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE public.closest(anyelement, anyelement) (
  SFUNC = closest_step,
  STYPE = anyelement
);

CREATE TYPE nasdaq.exchange AS ENUM
    ('NYSE MKT', 'NYSE', 'NYSE ARCA', 'NASDAQ', 'IEXG', 'BATS', 'CHX');

CREATE TYPE nasdaq.financial_status AS ENUM
    ('Deficient', 'Delinquent', 'Bankrupt', 'Normal', 'Deficient and Bankrupt', 'Deficient and Delinquent', 'Delinquent and Bankrupt', 'Deficient, Delinquent, and Bankrupt');

CREATE TYPE nasdaq.market_category AS ENUM
    ('Global Select', 'Global', 'Capital');

CREATE TABLE nasdaq.symbol
(
    act_symbol text NOT NULL,
    security_name text NOT NULL,
    listing_exchange nasdaq.exchange NOT NULL,
    market_category nasdaq.market_category,
    is_etf boolean,
    round_lot_size integer NOT NULL,
    is_test_issue boolean NOT NULL,
    financial_status nasdaq.financial_status,
    cqs_symbol text,
    nasdaq_symbol text NOT NULL,
    is_next_shares boolean NOT NULL,
    last_seen date NOT NULL,
    CONSTRAINT symbol_pkey PRIMARY KEY (act_symbol)
);

CREATE TABLE nasdaq.earnings_calendar
(
    act_symbol text NOT NULL,
    period_end_date date NOT NULL,
    "date" date NOT NULL,
    "when" nasdaq."when" NULL,
    CONSTRAINT earnings_calendar_pk PRIMARY KEY (act_symbol, period_end_date),
    CONSTRAINT earnings_calendar_act_symbol_fkey FOREIGN KEY (act_symbol) REFERENCES nasdaq.symbol(act_symbol)
);
