#!/bin/bash

# Switched off by off.sh, back on with on.sh: the flag file CRON_OFF next to this script makes
# every cron tick exit here. The script keeps its name, so the git clone stays clean.
if [ -f "$(dirname "$(readlink -f "$0")")/CRON_OFF" ]; then
    echo "CRON_OFF present: tmdb-person-preprocess switched off (bash on.sh to switch it back on)."
    exit 0
fi

# Check if the tmdb-person-preprocess Docker container is running
if [ $(docker ps -q -f name=tmdb-person-preprocess) ]; then
    echo "tmdb-person-preprocess Docker container is already running."
else
    # Start the tmdb-person-preprocess container if it is not running
    cd /home/debian/docker/tmdb-person-preprocess
    docker build -t tmdb-person-preprocess-python-app .
    # Secrets are injected at runtime from a host-managed env file outside the app source tree;
    # they are NOT baked into the image (see .dockerignore).
    # docker run -it --rm --network="host" --env-file /home/debian/docker/tmdb-person-preprocess/.env --name tmdb-person-preprocess tmdb-person-preprocess-python-app
    docker run -d --rm --network="host" --env-file /home/debian/docker/tmdb-person-preprocess/.env --name tmdb-person-preprocess tmdb-person-preprocess-python-app
    echo "tmdb-person-preprocess Docker container started."
fi
