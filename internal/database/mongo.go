package database

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDriver struct {
	client *mongo.Client
}

type MongoRow struct {
	singleResult *mongo.SingleResult
}

func (mr *MongoRow) Scan(dest ...interface{}) error {
	return mr.singleResult.Decode(dest[0])
}

func (md *MongoDriver) Connect(dsn string) error {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(dsn))
	if err != nil {
		return err
	}
	md.client = client
	return nil
}

func (md *MongoDriver) Close() error {
	return md.client.Disconnect(context.Background())
}

func (md *MongoDriver) ExecuteTx(ctx context.Context, txFunc func(interface{}) error) error {
	session, err := md.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		if err := txFunc(sessCtx); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

func (md *MongoDriver) ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	collectionName, filter, update := parseMongoQuery(query, args...)
	collection := md.client.Database("benchmarkdb").Collection(collectionName)

	if update != nil {
		return collection.UpdateOne(ctx, filter, update)
	} else if filter != nil {
		return collection.InsertOne(ctx, filter)
	}

	return nil, nil
}

func (md *MongoDriver) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	collectionName, filter, _ := parseMongoQuery(query, args...)
	collection := md.client.Database("benchmarkdb").Collection(collectionName)
	singleResult := collection.FindOne(ctx, filter)
	return &MongoRow{singleResult: singleResult}
}

func parseMongoQuery(query string, args ...interface{}) (string, bson.M, bson.M) {
	switch query {
	case "UPDATE products SET inventory = inventory - 1 WHERE id = $1 AND inventory > 0":
		return "products", bson.M{"_id": args[0], "inventory": bson.M{"$gt": 0}}, bson.M{"$inc": bson.M{"inventory": -1}}
	case "SELECT inventory FROM products WHERE id = $1":
		return "products", bson.M{"_id": args[0]}, nil
	case "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)":
		return "products", bson.M{"_id": args[0], "name": args[1], "inventory": args[2]}, nil
	case "INSERT INTO orders (id, user_id, created_at) VALUES ($1, $2, $3)":
		return "orders", bson.M{"_id": args[0], "user_id": args[1], "created_at": args[2]}, nil
	case "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES ($1, $2, 'product1', 1)":
		return "orders", bson.M{"_id": args[1]}, bson.M{"$push": bson.M{"items": bson.M{"_id": args[0], "product_id": "product1", "quantity": 1}}}
	case "INSERT INTO payments (id, order_id, amount) VALUES ($1, $2, 10.50)":
		return "orders", bson.M{"_id": args[1]}, bson.M{"$set": bson.M{"payment": bson.M{"_id": args[0], "amount": 10.50}}}
	}
	return "", nil, nil
}
