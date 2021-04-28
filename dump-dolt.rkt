#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/tmp/dolt/stocks"))

(define as-of-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dolt.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/tmp/dolt/stocks"
                         (base-folder folder)]
 [("-d" "--date") date
                  "Final date for history retrieval. Defaults to today"
                  (as-of-date date)]
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

(define symbol-file (string-append (base-folder) "/symbol-" (as-of-date) ".csv"))

(call-with-output-file symbol-file
  (λ (out)
    (displayln "act_symbol,security_name,listing_exchange,market_category,is_etf,round_lot_size,is_test_issue,financial_status,cqs_symbol,nasdaq_symbol,is_next_shares,last_seen" out)
    (for-each (λ (row)
                (displayln (string-join (vector->list row) ",") out))
              (query-rows dbc "
select
  act_symbol::text,
  '\"' || security_name::text || '\"',
  listing_exchange::text,
  coalesce(market_category::text, ''),
  coalesce(is_etf::text, ''),
  round_lot_size::text,
  is_test_issue::text,
  coalesce('\"' || financial_status::text || '\"', ''),
  coalesce(cqs_symbol::text, ''),
  nasdaq_symbol::text,
  is_next_shares::text,
  last_seen::text
from
  nasdaq.symbol
")))
  #:exists 'replace)

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt table import -r symbol symbol-" (as-of-date) ".csv"))

(system (string-append "cd " (base-folder) "; /usr/local/bin/dolt add symbol; "
                       "/usr/local/bin/dolt commit -m 'symbol " (as-of-date) " update'; /usr/local/bin/dolt push"))
