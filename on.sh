#!/bin/bash
# Switch tmdb-person-preprocess.sh back on: removes the flag file CRON_OFF that off.sh wrote.
cd "$(dirname "$(readlink -f "$0")")" || exit 1
# A launcher renamed by the old off.sh (<name>-off.sh, before 2026-10-25) is put back,
# or dropped when git has already restored the tracked one.
[ -f tmdb-person-preprocess-off.sh ] && { [ -f tmdb-person-preprocess.sh ] && rm -f tmdb-person-preprocess-off.sh || mv tmdb-person-preprocess-off.sh tmdb-person-preprocess.sh; }
rm -f CRON_OFF && echo "tmdb-person-preprocess: on (CRON_OFF removed)"
