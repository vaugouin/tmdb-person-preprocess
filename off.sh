#!/bin/bash
# Switch tmdb-person-preprocess.sh off: it exits at its first test while the flag file CRON_OFF exists in
# this folder, so cron ticks start nothing. Undo with on.sh. Nothing is renamed (until
# 2026-10-25 this script renamed the launcher, which left the git clone dirty).
# A container already running is not stopped: docker stop tmdb-person-preprocess.
# The flag says who switched the task off: "manual" by default, "off-all" when off-all.sh
# calls this script (CRON_OFF_BY=off-all). on-all.sh switches back on only the "off-all" ones.
strby=${CRON_OFF_BY:-manual}
echo "$strby $(date '+%Y-%m-%d %H:%M')" > "$(dirname "$(readlink -f "$0")")/CRON_OFF" && echo "tmdb-person-preprocess: off (CRON_OFF written, by $strby)"
