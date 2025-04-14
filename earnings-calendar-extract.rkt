#lang racket/base

(require gregor
         gregor/period
         gregor/time
         net/http-easy
         racket/cmdline
         racket/file
         racket/list
         racket/port
         tasks
         threading)

(define (download-day date)
  (make-directory* (string-append "/var/tmp/nasdaq/earnings-calendar/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file* (string-append "/var/tmp/nasdaq/earnings-calendar/" (~t (today) "yyyy-MM-dd") "/"
                                        (~t date "yyyy-MM-dd") ".json")
    (λ (out)
      (with-handlers ([exn:fail?
                       (λ (error)
                         (displayln (string-append "Encountered error for " (~t date "yyyy-MM-dd")))
                         (displayln error))])
        (~> (string-append "https://api.nasdaq.com/api/calendar/earnings?date=" (~t date "yyyy-MM-dd"))
            (get _)
            (response-body _)
            (write-bytes _ out))))
    #:exists 'replace))

(define end-date (make-parameter (+days (today) (* 7 6))))

(define start-date (make-parameter (-days (today) 7)))

(command-line
 #:program "racket earnings-calendar-extract.rkt"
 #:once-each
 [("-e" "--end-date") ed
                      "End date. Defaults to today + 6 weeks"
                      (end-date (iso8601->date ed))]
 [("-s" "--start-date") sd
                        "Start date. Defaults to today - 1 week"
                        (start-date (iso8601->date sd))])

(define delay-interval 10)

(with-task-server (for-each (λ (i) (schedule-delayed-task (λ () (download-day (+days (start-date) i)))
                                                          (* i delay-interval)))
                            (range 0 (period-ref (period-between (start-date) (end-date) '(days)) 'days)))
  ; add a final task that will halt the task server
  (schedule-delayed-task
   (λ () (schedule-stop-task)) (* delay-interval (period-ref (period-between (start-date) (end-date) '(days)) 'days)))
  (run-tasks))
