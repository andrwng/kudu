#!/usr/bin/env bash
if [ "$#" = "0" ]; then
  echo "Usage: $0 major|minor|patch|full"
  exit 1
fi

if [ "$1" = "major" ]; then
  cat version.h  | grep MAJOR | head -n1 | awk '{print $3}'
fi
if [ "$1" = "minor" ]; then
  cat version.h  | grep MINOR | head -n1 | awk '{print $3}'
fi
if [ "$1" = "patch" ]; then
  cat version.h  | grep PATCH | head -n1 | awk '{print $3}'
fi
if [ "$1" = "full" ]; then
  awk '/#define ROCKSDB/ { env[$2] = $3 }
       END { printf "%s.%s.%s\n", env["ROCKSDB_MAJOR"],
                                  env["ROCKSDB_MINOR"],
                                  env["ROCKSDB_PATCH"] }'  \
      version.h
fi
