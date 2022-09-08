#!/bin/sh

if [ "$MODE" = "COVERAGE" ]; then
  echo "[!] RUNNING COVERAGE MODE"
  coverage run -m unittest discover
  coverage report
else
    echo "[!] RUNNING TEST MODE"
    exec python -m unittest
fi