package analytics

func GetEventsSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS events (
			event_id VARCHAR(255) PRIMARY KEY,
			event_timestamp TIMESTAMP NOT NULL,
			user_id VARCHAR(255) NOT NULL,
			product_id VARCHAR(255) NOT NULL,
			region VARCHAR(255) NOT NULL,
			metric_value DECIMAL(10, 2) NOT NULL
		);
	`
}

/*
MongoDB document structure:

events: {
  _id: <string>,
  event_timestamp: <date>,
  user_id: <string>,
  product_id: <string>,
  region: <string>,
  metric_value: <number>
}

*/
