package ecommerce

func GetProductSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS products (
			id VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			inventory INT NOT NULL
		);
	`
}

func GetOrdersSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS orders (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
	`
}

func GetOrderItemsSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS order_items (
			id VARCHAR(255) PRIMARY KEY,
			order_id VARCHAR(255) NOT NULL,
			product_id VARCHAR(255) NOT NULL,
			quantity INT NOT NULL
		);
	`
}

func GetPaymentsSchema() string {
	return `
		CREATE TABLE IF NOT EXISTS payments (
			id VARCHAR(255) PRIMARY KEY,
			order_id VARCHAR(255) NOT NULL,
			amount DECIMAL(10, 2) NOT NULL
		);
	`
}

/*
MongoDB document structure:

products: {
  _id: <string>,
  name: <string>,
  inventory: <number>
}

orders: {
  _id: <string>,
  user_id: <string>,
  created_at: <date>,
  items: [
    {
      product_id: <string>,
      quantity: <number>
    }
  ],
  payment: {
    _id: <string>,
    amount: <number>
  }
}

*/
