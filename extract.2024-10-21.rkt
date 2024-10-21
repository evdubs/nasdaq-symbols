#lang racket/base

(require gregor
         net/http-easy
         threading)

(call-with-output-file* (string-append "/var/tmp/nasdaq/nasdaqtraded." (date->iso8601 (today)) ".txt")
  (λ (out) (~> (get "https://nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt")
               (response-body _)
               (write-bytes _ out)))
  #:exists 'replace)
