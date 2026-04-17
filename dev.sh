#!/bin/bash

case "$1" in
  start)
    docker start jobplat-control-plane
    ;;
  stop)
    docker stop jobplat-control-plane
    ;;
  reset)
    kind delete cluster --name jobplat
    ;;
  *)
    echo "Usage: ./dev.sh [start|stop|reset]"
    ;;
esac
