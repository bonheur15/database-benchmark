# Database Benchmark

This is a database benchmark tool to compare the performance of PostgreSQL, MySQL, and MongoDB under realistic, application-specific workloads.

## Prerequisites

- Docker
- Docker Compose
- Go

## Getting Started

1. **Start the databases:**

   ```bash
   ./scripts/control.sh up
   ```

2. **Build the benchmark tool:**

   ```bash
   go build -o benchmark-runner cmd/benchmark-runner/main.go
   ```

3. **Run the benchmark:**

   ```bash
   ./benchmark-runner --db=postgres --workload=ecommerce --test=order_processing
   ```

## Workloads

### E-Commerce Platform

- `order_processing`: OLTP test for order processing.
- `inventory_update`: High-concurrency inventory update test.
- `catalog_filter`: Product catalog filter query test.

### Social Media App

- `join_on_read`: "Pull" model for reading a user's timeline.
- `fan_out_on_write`: "Push" model for writing to a user's timeline.

### Analytics Platform

- `ingestion`: High-throughput data ingestion test.
- `dashboard_query`: Dashboard OLAP query test.

## Cleaning Up

To stop and remove the database containers, run:

```bash
./scripts/control.sh down
```
