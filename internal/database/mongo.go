package database

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDriver struct {
	client *mongo.Client
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
	// MongoDB doesn't have a generic ExecContext like SQL databases.
	// This method is a placeholder and should be adapted for specific MongoDB operations.
	return nil, nil
}
