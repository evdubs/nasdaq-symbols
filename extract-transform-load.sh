#!/usr/bin/env bash

dir=$(dirname "$0")

racket ${dir}/extract.rkt
racket ${dir}/transform-load.rkt -p "$1"
