#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/list
         racket/sequence
         threading)

(struct symbol-entry
  (nasdaq-traded
   act-symbol
   security-name
   listing-exchange
   market-category
   is-etf
   round-lot-size
   is-test-issue
   financial-status
   cqs-symbol
   nasdaq-symbol
   next-shares))

(define file-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load.rkt"
 #:once-each
 [("-d" "--file-date") date-str
                       "Nasdaq file date. Defaults to today"
                       (file-date (iso8601->date date-str))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define insert-counter 0)

(define nasdaq-traded-symbols
  (with-input-from-file
      (string-append "/var/tmp/nasdaq/nasdaqtraded."
                     (~t (file-date) "yyyy-MM-dd")
                     ".txt")
    (λ ()
      (~> (in-lines)
          (sequence-map (λ (el) (regexp-split #rx"\\|" el)) _)
          (sequence-filter (λ (el) (or (equal? "Y" (first el))
                                       (equal? "N" (first el)))) _)
          (sequence-for-each
           (λ (el)
             ; (displayln el)
             ; (flush-output)
             (let ([se (apply symbol-entry el)])
               (query-exec dbc
                           "
with le as (
  select case $3
    when 'A' then 'NYSE MKT'::nasdaq.exchange
    when 'N' then 'NYSE'::nasdaq.exchange
    when 'P' then 'NYSE ARCA'::nasdaq.exchange
    when 'Q' then 'NASDAQ'::nasdaq.exchange
    when 'V' then 'IEXG'::nasdaq.exchange
    when 'Z' then 'BATS'::nasdaq.exchange
  end as listing_exchange
), mc as (
  select case $4
    when 'G' then 'Global'::nasdaq.market_category
    when 'Q' then 'Global Select'::nasdaq.market_category
    when 'S' then 'Capital'::nasdaq.market_category
    when ' ' then NULL
  end as market_category
), ie as (
  select case $5
    when 'Y' then true
    when 'N' then false
    when ' ' then NULL
  end as is_etf
), iti as (
  select case $7
    when 'Y' then true
    when 'N' then false
  end as is_test_issue
), fs as (
  select case $8
    when 'D' then 'Deficient'::nasdaq.financial_status
    when 'E' then 'Delinquent'::nasdaq.financial_status
    when 'G' then 'Deficient and Bankrupt'::nasdaq.financial_status
    when 'H' then 'Deficient and Delinquent'::nasdaq.financial_status
    when 'J' then 'Delinquent and Bankrupt'::nasdaq.financial_status
    when 'K' then 'Deficient, Delinquent, and Bankrupt'::nasdaq.financial_status
    when 'N' then 'Normal'::nasdaq.financial_status
    when 'Q' then 'Bankrupt'::nasdaq.financial_status
    when '' then NULL
  end as financial_status
), cqss as (
  select case $9
    when '' then NULL
    else $9
  end as cqs_symbol
), ins as (
  select case $11
    when 'Y' then true
    when 'N' then false
  end as is_next_shares
)
insert into nasdaq.symbol
(
  act_symbol,
  security_name,
  listing_exchange,
  market_category,
  is_etf,
  round_lot_size,
  is_test_issue,
  financial_status,
  cqs_symbol,
  nasdaq_symbol,
  is_next_shares,
  last_seen
) values (
  $1,
  $2,
  (select listing_exchange from le),
  (select market_category from mc),
  (select is_etf from ie),
  $6,
  (select is_test_issue from iti),
  (select financial_status from fs),
  (select cqs_symbol from cqss),
  $10,
  (select is_next_shares from ins),
  $12::text::date
) on conflict (act_symbol) do update set
  security_name = $2,
  listing_exchange = (select listing_exchange from le),
  market_category = (select market_category from mc),
  is_etf = (select is_etf from ie),
  round_lot_size = $6,
  is_test_issue = (select is_test_issue from iti),
  financial_status = (select financial_status from fs),
  cqs_symbol = (select cqs_symbol from cqss),
  nasdaq_symbol = $10,
  is_next_shares = (select is_next_shares from ins),
  last_seen = $12::text::date;
"
                           (symbol-entry-act-symbol se)
                           (symbol-entry-security-name se)
                           (symbol-entry-listing-exchange se)
                           (symbol-entry-market-category se)
                           (symbol-entry-is-etf se)
                           (string->number (symbol-entry-round-lot-size se))
                           (symbol-entry-is-test-issue se)
                           (symbol-entry-financial-status se)
                           (symbol-entry-cqs-symbol se)
                           (symbol-entry-nasdaq-symbol se)
                           (symbol-entry-next-shares se)
                           (~t (file-date) "yyyy-MM-dd"))
               (set! insert-counter (add1 insert-counter)))) _)))))

(disconnect dbc)

(displayln (string-append "Inserted or updated " (number->string insert-counter) " rows"))
