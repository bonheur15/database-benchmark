#!/bin/bash

DATABASES=("postgres" "mysql" "mongo")
WORKLOADS=("ecommerce" "socialmedia" "analytics")

for db in "${DATABASES[@]}"; do
  for workload in "${WORKLOADS[@]}"; do
    case $workload in
      "ecommerce")
        tests=("order_processing" "inventory_update" "catalog_filter")
        ;;
      "socialmedia")
        tests=("join_on_read" "fan_out_on_write")
        ;;
      "analytics")
        tests=("ingestion" "dashboard_query")
        ;;
    esac
    for test in "${tests[@]}"; do
      echo "Running benchmark for $db/$workload/$test..."
      ./benchmark-runner --db=$db --workload=$workload --test=$test
    done
  done
done
