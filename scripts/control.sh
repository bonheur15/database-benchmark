#!/bin/bash

set -e

case "$1" in
  up)
    sudo docker compose up -d
    ;;
  down)
    sudo docker compose down
    ;;
  *)
    echo "Usage: $0 {up|down}"
    exit 1
    ;;
esac
