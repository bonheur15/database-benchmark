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

	return err
}

func (md *MongoDriver) ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	collection := md.client.Database("benchmarkdb").Collection(query)

	if len(args) == 1 {
		if sessCtx, ok := ctx.Value("tx").(mongo.SessionContext); ok {
			return collection.InsertOne(sessCtx, args[0])
		}
		return collection.InsertOne(ctx, args[0])
	} else if len(args) == 2 {
		if sessCtx, ok := ctx.Value("tx").(mongo.SessionContext); ok {
			return collection.UpdateOne(sessCtx, args[0], args[1])
		}
		return collection.UpdateOne(ctx, args[0], args[1])
	} else if len(args) == 0 {
		if sessCtx, ok := ctx.Value("tx").(mongo.SessionContext); ok {
			return collection.DeleteMany(sessCtx, bson.M{})
		}
		return collection.DeleteMany(ctx, bson.M{})
	}

	return nil, nil
}

func (md *MongoDriver) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	collection := md.client.Database("benchmarkdb").Collection(query)
	var cursor *mongo.Cursor
	var err error
	if sessCtx, ok := ctx.Value("tx").(mongo.SessionContext); ok {
		cursor, err = collection.Find(sessCtx, args[0])
	} else {
		cursor, err = collection.Find(ctx, args[0])
	}
	if err != nil {
		return nil, err
	}
	return &MongoRows{cursor: cursor}, nil
}

func (md *MongoDriver) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	collection := md.client.Database("benchmarkdb").Collection(query)
	var singleResult *mongo.SingleResult
	if sessCtx, ok := ctx.Value("tx").(mongo.SessionContext); ok {
		singleResult = collection.FindOne(sessCtx, args[0])
	} else {
		singleResult = collection.FindOne(ctx, args[0])
	}
	return &MongoRow{singleResult: singleResult}
}