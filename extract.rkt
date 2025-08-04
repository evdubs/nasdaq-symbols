#lang racket/base

(require gregor
         net/ftp)

(define nasdaq-ftp (ftp-establish-connection "ftp.nasdaqtrader.com" 21 "anonymous" "anonymous"))
(ftp-cd nasdaq-ftp "SymbolDirectory")
(ftp-download-file nasdaq-ftp
                   "/var/local/nasdaq"
                   "nasdaqtraded.txt")

(rename-file-or-directory "/var/local/nasdaq/nasdaqtraded.txt"
                          (string-append "/var/local/nasdaq/nasdaqtraded."
                                         (~t (today) "yyyy-MM-dd")
                                         ".txt"))
