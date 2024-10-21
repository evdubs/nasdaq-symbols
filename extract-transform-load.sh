#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%Y-%m-%d")
current_year=$(date "+%Y")

racket -y ${dir}/extract.2024-10-21.rkt
racket -y ${dir}/transform-load.rkt -p "$1"

7zr a /var/tmp/nasdaq/${current_year}.7z /var/tmp/nasdaq/nasdaqtraded.${today}.txt

racket -y ${dir}/dump-dolt.rkt -p "$1"
