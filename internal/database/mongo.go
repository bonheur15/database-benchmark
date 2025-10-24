package database

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
)

type MongoDriver struct {
	client *mongo.Client
	dsn    string
}

type MongoRow struct {
	singleResult *mongo.SingleResult
}

func (mr *MongoRow) Scan(dest ...interface{}) error {
	return mr.singleResult.Decode(dest[0])
}

type MongoRows struct {
	cursor *mongo.Cursor
}

func (mr *MongoRows) Next() bool {
	return mr.cursor.Next(context.Background())
}

func (mr *MongoRows) Scan(dest ...interface{}) error {
	return mr.cursor.Decode(dest[0])
}

func (mr *MongoRows) Close() {
	mr.cursor.Close(context.Background())
}

func (md *MongoDriver) Connect(dsn string) error {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(dsn))
	if err != nil {
		return err
	}
	md.client = client
	md.dsn = dsn
	return nil
}

func (md *MongoDriver) Reset(ctx context.Context) error {
	return md.client.Database("benchmarkdb").Drop(ctx)
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

	// If the error is due to transactions not being supported (e.g., standalone server),
	// execute the function directly without a transaction.
	if err != nil && strings.Contains(err.Error(), "Transaction numbers are only allowed on a replica set member or mongos") {
		return txFunc(ctx)
	}

	return err
}

func (md *MongoDriver) ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	collection := md.client.Database("benchmarkdb").Collection(query)

	if len(args) == 1 {
		return collection.InsertOne(ctx, args[0])
	} else if len(args) == 2 {
		return collection.UpdateOne(ctx, args[0], args[1])
	} else if len(args) == 0 {
		return collection.DeleteMany(ctx, bson.M{})
	}

	return nil, nil
}

func (md *MongoDriver) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	collection := md.client.Database("benchmarkdb").Collection(query)
	var cursor *mongo.Cursor
	var err error
	cursor, err = collection.Find(ctx, args[0])
	if err != nil {
		return nil, err
	}
	return &MongoRows{cursor: cursor}, nil
}

func (md *MongoDriver) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	collection := md.client.Database("benchmarkdb").Collection(query)
	var singleResult *mongo.SingleResult
	singleResult = collection.FindOne(ctx, args[0])
	return &MongoRow{singleResult: singleResult}
}