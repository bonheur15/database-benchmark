# Database Benchmark Test Results

This file contains the results of running all benchmark tests across PostgreSQL, MySQL, and MongoDB.

## Test Commands

The following commands were executed for each test:

### PostgreSQL Tests
1. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=postgres --workload=analytics --test=dashboard_query`
2. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=postgres --workload=analytics --test=ingestion`
3. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=postgres --workload=ecommerce --test=catalog_filter`
4. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=postgres --workload=ecommerce --test=inventory_update`
5. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=postgres --workload=ecommerce --test=order_processing`
6. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=postgres --workload=socialmedia --test=fan_out_on_write`
7. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=postgres --workload=socialmedia --test=join_on_read`

### MySQL Tests
8. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=analytics --test=dashboard_query`
9. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=analytics --test=ingestion`
10. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=ecommerce --test=catalog_filter`
11. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=ecommerce --test=inventory_update`
12. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=ecommerce --test=order_processing`
13. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=socialmedia --test=fan_out_on_write`
14. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=socialmedia --test=join_on_read`

### MongoDB Tests
15. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mongo --workload=analytics --test=dashboard_query`
16. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mongo --workload=analytics --test=ingestion`
17. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mongo --workload=ecommerce --test=catalog_filter`
18. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mongo --workload=ecommerce --test=inventory_update`
19. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mongo --workload=ecommerce --test=order_processing`
20. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mongo --workload=socialmedia --test=fan_out_on_write`
21. `go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mongo --workload=socialmedia --test=join_on_read`

## Results

### PostgreSQL

#### Analytics - Dashboard Query

```
{
  "Operations": 15983,
  "Errors": 0,
  "Throughput": 532.7666666666667,
  "P95Latency": 236000000,
  "P99Latency": 264000000,
  "AverageLatency": 187000000,
  "ErrorRate": 0,
  "TotalTime": 30000000000,
  "DataIntegrity": false
}
```

#### Analytics - Ingestion

```
{
  "Operations": 0,
  "Errors": 0,
  "Throughput": 15841.212692329891,
  "P95Latency": 0,
  "P99Latency": 0,
  "AverageLatency": 0,
  "ErrorRate": 0,
  "TotalTime": 6312648024,
  "DataIntegrity": false
}
```

#### Ecommerce - Catalog Filter

```
{
  "Operations": 181589,
  "Errors": 0,
  "Throughput": 6052.966666666666,
  "P95Latency": 23000000,
  "P99Latency": 29000000,
  "AverageLatency": 16000000,
  "ErrorRate": 0,
  "TotalTime": 30000000000,
  "DataIntegrity": false
}
```

#### Ecommerce - Inventory Update

```
{
  "Operations": 10000,
  "Errors": 98,
  "Throughput": 689.9185685256924,
  "P95Latency": 0,
  "P99Latency": 0,
  "AverageLatency": 0,
  "ErrorRate": 0,
  "TotalTime": 14494464211,
  "DataIntegrity": true
}
```

#### Ecommerce - Order Processing

```
{
  "Operations": 9553,
  "Errors": 0,
  "Throughput": 318.4015530375216,
  "P95Latency": 4539000,
  "P99Latency": 5927000,
  "AverageLatency": 3140000,
  "ErrorRate": 0,
  "TotalTime": 30002994360,
  "DataIntegrity": false
}
```

#### Socialmedia - Fan Out On Write

#### Socialmedia - Join On Read

### MySQL

#### Analytics - Dashboard Query

#### Analytics - Ingestion

#### Ecommerce - Catalog Filter

#### Ecommerce - Inventory Update

#### Ecommerce - Order Processing

#### Socialmedia - Fan Out On Write

#### Socialmedia - Join On Read

### MongoDB

#### Analytics - Dashboard Query

#### Analytics - Ingestion

#### Ecommerce - Catalog Filter

#### Ecommerce - Inventory Update

#### Ecommerce - Order Processing

#### Socialmedia - Fan Out On Write

#### Socialmedia - Join On Read

## Notes

- Any failures or issues encountered during testing are noted below.
- If tests failed, fixes were applied and tests re-run.