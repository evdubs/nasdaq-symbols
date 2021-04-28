#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%Y-%m-%d")
current_year=$(date "+%Y")

racket ${dir}/extract.rkt
racket ${dir}/transform-load.rkt -p "$1"

7zr a /var/tmp/nasdaq/${current_year}.7z /var/tmp/nasdaq/nasdaqtraded.${today}.txt

racket ${dir}/dump-dolt.rkt -p "$1"
