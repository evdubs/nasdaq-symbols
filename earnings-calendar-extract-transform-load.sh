#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")
current_year=$(date "+%Y")

racket -y ${dir}/earnings-calendar-extract.rkt
racket -y ${dir}/earnings-calendar-transform-load.rkt -p "$1"

7zr a /var/tmp/nasdaq/earnings-calendar/${current_year}.7z /var/tmp/nasdaq/earnings-calendar/${today}

# racket -y ${dir}/dump-dolt-calendar.rkt -p "$1"
