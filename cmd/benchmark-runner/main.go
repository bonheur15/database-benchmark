package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"database-benchmark/internal/config"
	"database-benchmark/internal/database"
	"database-benchmark/internal/runner"
	"database-benchmark/internal/workloads/analytics"
	"database-benchmark/internal/workloads/ecommerce"
	"database-benchmark/internal/workloads/socialmedia"
)

func main() {
	dbType := flag.String("db", "postgres", "database type (postgres, mysql, or mongo)")
	workloadName := flag.String("workload", "ecommerce", "workload to run (ecommerce, socialmedia, or analytics)")
	testName := flag.String("test", "order_processing", "test to run")
	concurrency := flag.Int("concurrency", 100, "number of concurrent requests")
	duration := flag.Duration("duration", 30*time.Second, "duration of the test")

	flag.Parse()

	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	dbs := map[string]database.DatabaseDriver{
		"postgres": &database.PostgresDriver{},
		"mysql":    &database.MySQLDriver{},
		"mongo":    &database.MongoDriver{},
	}

	driver, ok := dbs[*dbType]
	if !ok {
		log.Fatalf("Unsupported database type: %s", *dbType)
	}

	var dsn string
	switch *dbType {
	case "postgres":
		dsn = cfg.Databases.Postgres
	case "mysql":
		dsn = cfg.Databases.MySQL
	case "mongo":
		dsn = cfg.Databases.Mongo
	}

	if err := driver.Connect(dsn); err != nil {
		log.Fatalf("Failed to connect to %s: %v", *dbType, err)
	}
	defer driver.Close()

	workloads := map[string]map[string]database.Workload{
		"ecommerce": {
			"order_processing": &ecommerce.OrderProcessingTest{},
			"inventory_update": &ecommerce.InventoryUpdateTest{},
			"catalog_filter":   &ecommerce.CatalogFilterTest{},
		},
		"socialmedia": {
			"join_on_read":     &socialmedia.JoinOnReadTest{},
			"fan_out_on_write": &socialmedia.FanOutOnWriteTest{},
		},
		"analytics": {
			"ingestion":       &analytics.IngestionTest{},
			"dashboard_query": &analytics.DashboardQueryTest{},
		},
	}

	workload, ok := workloads[*workloadName][*testName]
	if !ok {
		log.Fatalf("Unsupported workload/test: %s/%s", *workloadName, *testName)
	}

	fmt.Printf("Running benchmark for %s/%s on %s...\n", *workloadName, *testName, *dbType)

	result, err := runner.Run(context.Background(), driver, workload, *concurrency, *duration)
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	fmt.Printf("Result: %+v\n", result)
}
