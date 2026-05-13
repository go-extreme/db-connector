package main

import (
	"context"
	"log"

	dbconnector "github.com/go-extreme/db-connector/v5"
	"github.com/jmoiron/sqlx"
)

func main() {
	readConfig := &dbconnector.Config{
		Host:              "localhost",
		Port:              5432,
		User:              "postgres",
		Password:          "Root1234",
		Database:          "test",
		SSLMode:           "disable",
		MaxOpenConnection: 25,
		MaxIdleConnection: 5,
	}

	writeConfig := &dbconnector.Config{
		Host:                 "localhost",
		Port:                 5432,
		User:                 "postgres",
		Password:             "Root1234",
		Database:             "test",
		SSLMode:              "disable",
		MaxOpenConnection:    10,
		MaxIdleConnection:    2,
		AutoDatabaseCreation: true,
	}

	readConn := dbconnector.NewPostgresConnection(readConfig)
	writeConn := dbconnector.NewPostgresConnection(writeConfig)

	connector := dbconnector.NewConnector(readConn, writeConn)

	ctx := context.Background()
	if err := connector.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer connector.Close()

	log.Println("Connected to database successfully")

	// Create table
	_, err := connector.Write().DB().Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY, 
			name VARCHAR(100)
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Example: Using transaction with new API
	type User struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	users := dbconnector.NewModel[User](connector, "users")

	err = users.WriteTransaction().Execute(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.Exec("INSERT INTO users (name) VALUES ($1)", "John Doe")
		return err
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Println("ReadTransaction completed successfully")
}
