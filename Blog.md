# issues faced
- on go build -o benchmark-runner cmd/benchmark-runner/main.go && ./benchmark-runner --db=mysql --workload=analytics --test=dashboard_query
mysql was slow insterting record soo much that we couldnt determine why its slow and cant use multi threads too
- Most test on posgres was not passing integrity test, i had to go back in data and see where data are not inserted correctly or updated incorrectly
