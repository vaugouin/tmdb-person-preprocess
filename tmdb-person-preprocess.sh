#!/bin/bash

# --- Run log on the host (VPS-wide convention, first in sqlite-plex-to-tmdb/sqlite.sh) ---
# `docker run -d --rm` sent the container's output to Docker's log driver, and --rm
# destroyed it on exit: after a crash there was nothing left to read. f_runlogged runs
# the container in the FOREGROUND of a backgrounded block whose output goes to
#   /home/debian/docker/shared_data/logs/<process>/<YYYYMMDD-HHMMSS>.log   (kept 30 days)
#   /home/debian/docker/shared_data/logs/<process>/latest.log              (the last run)
# The last line is "=== <process> exited with status N at <date> ===": the ops dashboard
# of data-monitoring reads it (N > 0 = FAILED). `docker logs -f <name>` still works
# while the container runs; `tail -f .../latest.log` works during and after.
# Usage: f_runlogged <process> <docker run arguments, without -d>
f_runlogged() {
    local process="$1"; shift
    local dir="/home/debian/docker/shared_data/logs/$process"
    local log name="" prev="" arg
    mkdir -p "$dir"
    log="$dir/$(date '+%Y%m%d-%H%M%S').log"
    : > "$log"
    ln -sfn "$log" "$dir/latest.log"
    find "$dir" -maxdepth 1 -type f -name '*.log' -mtime +30 -delete 2>/dev/null
    for arg in "$@"; do [ "$prev" = "--name" ] && name="$arg"; prev="$arg"; done
    {
        trap '' HUP
        echo "=== $process started $(date '+%Y-%m-%d %H:%M:%S') ==="
        docker run -e PYTHONUNBUFFERED=1 "$@"
        status=$?
        echo "=== $process exited with status $status at $(date '+%Y-%m-%d %H:%M:%S') ==="
    } >> "$log" 2>&1 < /dev/null &
    disown 2>/dev/null
    # Return once the container exists, so a `docker ps` guard or a `docker logs -f`
    # right after this call sees it (60 s at most).
    if [ -n "$name" ]; then
        for _ in $(seq 1 60); do
            [ -n "$(docker ps -a -q -f name="^${name}$")" ] && break
            sleep 1
        done
    fi
    echo "$process: container started in the background, run log $log"
}

# Switched off by off.sh, back on with on.sh: the flag file CRON_OFF next to this script makes
# every cron tick exit here. The script keeps its name, so the git clone stays clean.
if [ -f "$(dirname "$(readlink -f "$0")")/CRON_OFF" ]; then
    echo "CRON_OFF present: tmdb-person-preprocess switched off (bash on.sh to switch it back on)."
    exit 0
fi

# Check if the tmdb-person-preprocess Docker container is running
if [ -n "$(docker ps -q -f name=^tmdb-person-preprocess$)" ]; then
    echo "tmdb-person-preprocess Docker container is already running."
else
    # Start the tmdb-person-preprocess container if it is not running
    cd /home/debian/docker/tmdb-person-preprocess
    docker build -t tmdb-person-preprocess-python-app .
    # Secrets are injected at runtime from a host-managed env file outside the app source tree;
    # they are NOT baked into the image (see .dockerignore).
    # docker run -it --rm --network="host" --env-file /home/debian/docker/tmdb-person-preprocess/.env --name tmdb-person-preprocess tmdb-person-preprocess-python-app
    f_runlogged tmdb-person-preprocess --rm --network="host" --env-file /home/debian/docker/tmdb-person-preprocess/.env --name tmdb-person-preprocess tmdb-person-preprocess-python-app
    echo "tmdb-person-preprocess Docker container started."
fi
