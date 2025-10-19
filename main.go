package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"database-benchmark/internal/config"
	"database-benchmark/internal/database"
	"database-benchmark/internal/runner"
	"database-benchmark/internal/workloads/analytics"
	"database-benchmark/internal/workloads/ecommerce"
	"database-benchmark/internal/workloads/socialmedia"
)

func BenchmarkRunner(b *testing.B) {
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		b.Fatalf("Failed to load config: %v", err)
	}

	dbs := map[string]database.DatabaseDriver{
		"postgres": &database.PostgresDriver{},
		"mysql":    &database.MySQLDriver{},
		"mongo":    &database.MongoDriver{},
	}

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

	for dbType, driver := range dbs {
		var dsn string
		switch dbType {
		case "postgres":
			dsn = cfg.Databases.Postgres
		case "mysql":
			dsn = cfg.Databases.MySQL
		case "mongo":
			dsn = cfg.Databases.Mongo
		}

		if err := driver.Connect(dsn); err != nil {
			b.Fatalf("Failed to connect to %s: %v", dbType, err)
		}
		defer driver.Close()

		for workloadName, workloadTests := range workloads {
			for testName, w := range workloadTests {
				b.Run(fmt.Sprintf("%s/%s/%s", dbType, workloadName, testName), func(b *testing.B) {
					result, err := runner.Run(context.Background(), driver, w, 100, 30*time.Second)
					if err != nil {
						b.Fatalf("Benchmark failed: %v", err)
					}
					b.Logf("Result: %+v", result)
				})
			}
		}
	}
}
