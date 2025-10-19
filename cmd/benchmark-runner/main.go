package main

import (
	"context"
	"database-benchmark/internal/config"
	"database-benchmark/internal/database"
	"database-benchmark/internal/runner"
	"database-benchmark/internal/workloads/analytics"
	"database-benchmark/internal/workloads/ecommerce"
	"database-benchmark/internal/workloads/socialmedia"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	dbType := flag.String("db", "", "database type (postgres, mysql, or mongo)")
	workload := flag.String("workload", "", "workload to run (ecommerce, socialmedia, or analytics)")
	test := flag.String("test", "", "test to run")
	concurrency := flag.Int("concurrency", 100, "number of concurrent workers")
	durationStr := flag.String("duration", "30s", "duration of the test")
	configFile := flag.String("config", "config.yaml", "path to the config file")

	flag.Parse()

	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	var driver database.DatabaseDriver
	var dsn string

	switch *dbType {
	case "postgres":
		driver = &database.PostgresDriver{}
		dsn = cfg.Databases.Postgres
	case "mysql":
		driver = &database.MySQLDriver{}
		dsn = cfg.Databases.MySQL
	case "mongo":
		driver = &database.MongoDriver{}
		dsn = cfg.Databases.Mongo
	default:
		log.Fatalf("Unsupported database type: %s", *dbType)
	}

	if err := driver.Connect(dsn); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer driver.Close()

	var w database.Workload

	switch *workload {
	case "ecommerce":
		switch *test {
		case "order_processing":
			w = &ecommerce.OrderProcessingTest{}
		case "inventory_update":
			w = &ecommerce.InventoryUpdateTest{}
		case "catalog_filter":
			w = &ecommerce.CatalogFilterTest{}
		default:
			log.Fatalf("Unsupported ecommerce test: %s", *test)
		}
	case "socialmedia":
		switch *test {
		case "join_on_read":
			w = &socialmedia.JoinOnReadTest{Follows: 100}
		case "fan_out_on_write":
			w = &socialmedia.FanOutOnWriteTest{}
		default:
			log.Fatalf("Unsupported socialmedia test: %s", *test)
		}
	case "analytics":
		switch *test {
		case "ingestion":
			w = &analytics.IngestionTest{}
		case "dashboard_query":
			w = &analytics.DashboardQueryTest{}
		default:
			log.Fatalf("Unsupported analytics test: %s", *test)
		}
	default:
		log.Fatalf("Unsupported workload: %s", *workload)
	}

	duration, err := time.ParseDuration(*durationStr)
	if err != nil {
		log.Fatalf("Invalid duration: %v", err)
	}

	result, err := runner.Run(context.Background(), driver, w, *concurrency, duration)
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("Failed to marshal result: %v", err)
	}

	fmt.Println(string(jsonResult))
}
