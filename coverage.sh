#!/bin/bash

mkdir -p /tmp/coverage ; rm -rf /tmp/coverage/*
BISECT_FILE=/tmp/coverage/data dune runtest --instrument-with bisect_ppx --force
bisect-ppx-report summary --coverage-path /tmp/coverage

if [ -n "$1" ]; then
  bisect-ppx-report html --coverage-path /tmp/coverage --theme=dark -o $1
fi
