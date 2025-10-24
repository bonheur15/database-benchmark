# issues faced
- on go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=analytics --test=dashboard_query
mysql was slow insterting record soo much that we couldnt determine why its slow and cant use multi threads too
- Most test on posgres was not passing integrity test, i had to go back in data and see where data are not inserted correctly or updated incorrectly
- fanoutonwrite on mysql would create wierd connection buffer resulting into
│    [mysql] 2025/10/24 19:01:06 packets.go:467: busy buffer                                              │
│                                                                                                         │
│    [mysql] 2025/10/24 19:01:06 packets.go:446: busy buffer                                              │
│                                                                                                         │
│    [mysql] 2025/10/24 19:01:06 connection.go:173: bad connection                                        │
│                                                                                                         │
│    [mysql] 2025/10/24 19:01:06 packets.go:446: busy buffer


- The Run function's "Read Phase" is kept failling because post_ids, a MySQL JSON column, is being scanned directly
   into []string. This won't work; I needed to scan it into []byte first, then json.Unmarshal it into []string. so i had to modify the fan_out_on write.go
