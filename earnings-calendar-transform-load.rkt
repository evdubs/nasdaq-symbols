#lang racket/base

(require db
         gregor
         json
         racket/cmdline
         racket/list
         racket/port
         racket/sequence
         racket/string
         threading)

(define base-folder (make-parameter "/var/tmp/nasdaq/earnings-calendar"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket earnings-calendar-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Earnings Calendar base folder. Defaults to /var/tmp/nasdaq/earnings-calendar"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Earnings Calendar folder date. Defaults to today"
                         (folder-date (iso8601->date date))]
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

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [date-of-earnings (string-replace (string-replace file-name (path->string (current-directory)) "") ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to parse "
                                                                      file-name
                                                                      " for date "
                                                                      date-of-earnings))
                                       (displayln e))])
            (~> (port->string in)
                (string->jsexpr _)
                (hash-ref _ 'data)
                (hash-ref _ 'rows)
                (for-each (λ (row)
                            (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to insert "
                                                                                        (hash-ref row 'symbol)
                                                                                        " for date "
                                                                                        date-of-earnings))
                                                         (displayln e))])
                              (query-exec dbc "
insert into nasdaq.earnings_calendar (
  act_symbol,
  period_end_date,
  date,
  \"when\"
) values (
  $1,
  $2::text::date,
  $3::text::date,
  case $4
    when 'time-after-hours' then 'After market close'::nasdaq.when
    when 'time-pre-market' then 'Before market open'::nasdaq.when
    when 'time-not-supplied' then NULL
  end
) on conflict (act_symbol, period_end_date) do
update set
  date = $3::text::date,
  \"when\" = case $4
    when 'time-after-hours' then 'After market close'::nasdaq.when
    when 'time-pre-market' then 'Before market open'::nasdaq.when
    else earnings_calendar.\"when\"
  end;
"
                                          (hash-ref row 'symbol)
                                          (date->iso8601 (-days (+months (parse-date (hash-ref row 'fiscalQuarterEnding) "LLL/yyyy") 1) 1))
                                          date-of-earnings
                                          (hash-ref row 'time)))) _))))))))

(disconnect dbc)
