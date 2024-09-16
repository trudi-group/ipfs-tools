#!/bin/bash

set -ex

user_id=$PUID
user_gid=$PGID
if [ -z "$PUID" ]; then
    echo "PUID unset, using default value of 1000"
    user_id=1000
fi
if [ -z "$PGID" ]; then
    echo "PGID unset, using default value of 1000"
    user_gid=1000
fi

traces_dir=$(yq '.disk_logging_directory' ./config/bitswap-monitoring-client-config.yaml)

if [ "$(id -u)" -eq 0 ]; then
  echo "Changing user to $user_id"
  if [ ! "$traces_dir" == "null" ]; then
    echo "Fixing permissions on logging directory $traces_dir..."
    # ensure traces directory is writable
    su-exec "$user_id" test -w "$traces_dir" || chown -R -- "$user_id:$user_gid" "$traces_dir"
  fi
  # restart script with new privileges
  exec su-exec "$user_id:$user_gid" "$0" "$@"
fi

# 2nd invocation with regular user
exec ./bitswap-monitoring-client "$@"