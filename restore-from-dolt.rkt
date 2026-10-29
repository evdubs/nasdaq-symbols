#lang racket/base

(require csv-reading
         db
         gregor
         racket/cmdline
         racket/list
         racket/port
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/local/dolt/stocks"))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket restore-from-dolt.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/local/dolt/stocks"
                         (base-folder folder)]
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

(define symbol-filename (string-append (base-folder) "/symbol.csv"))

(system (string-append "cd " (base-folder) "; "
                       "/usr/local/bin/dolt pull; "
                       "/usr/local/bin/dolt sql -r csv -q \"
select
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
from
  symbol;\" > " symbol-filename))

(csv-for-each (λ (row)
                (cond [(not (equal? "act_symbol" (first row)))
                       (query-exec dbc "
insert into nasdaq.symbol (
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
  $3::text::nasdaq.exchange,
  case $4
    when '' then null
    else $4::text::nasdaq.market_category
  end,
  case $5
    when '' then null
    when '0' then false
    when '1' then true
  end,
  case $6
    when '' then null
    else $6::integer
  end,
  case $7
    when '0' then false
    when '1' then true
  end,
  case $8
    when '' then null
    else $8::text::nasdaq.financial_status
  end,
  case $9
    when '' then null
    else $9
  end,
  $10,
  case $11
    when '0' then false
    when '1' then true
  end,
  $12::text::date
) on conflict (act_symbol) do
  update
    set last_seen = $12::text::date;
"
                            (first row)
                            (second row)
                            (third row)
                            (fourth row)
                            (fifth row)
                            (sixth row)
                            (seventh row)
                            (eighth row)
                            (ninth row)
                            (tenth row)
                            (eleventh row)
                            (twelfth row))]))
              (open-input-file symbol-filename))
