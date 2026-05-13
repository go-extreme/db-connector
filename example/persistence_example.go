package main

import (
	"context"
	"log"

	dbconnector "github.com/go-extreme/db-connector/v5"
	"github.com/jmoiron/sqlx"
)

func main() {
	type User struct {
		ID   string `db:"id"`
		Name string `db:"name"`
		Age  int    `db:"age"`
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

	writeConn := dbconnector.NewPostgresConnection(writeConfig)
	readConn := dbconnector.NewPostgresConnection(readConfig)

	connector := dbconnector.NewConnector(readConn, writeConn)

	ctx := context.Background()
	if err := connector.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer connector.Close()

	// Create table
	_, err := connector.Write().DB().Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id VARCHAR(50) PRIMARY KEY,
			name VARCHAR(100),
			age INT
		);
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize unified model
	users := dbconnector.NewModel[User](connector, "users")

	// Create user
	newUser := User{
		ID:   "1",
		Name: "John Doe",
		Age:  30,
	}
	if err := users.Create(ctx, newUser); err != nil {
		log.Printf("Create error: %v", err)
	}

	// Find user
	user, err := users.Find("1").Exec(ctx)
	if err != nil {
		log.Printf("Find error: %v", err)
	} else {
		log.Printf("Found user: %+v", user)
	}

	// Update user
	if err := users.Update(ctx, "1", map[string]interface{}{"age": 31}); err != nil {
		log.Printf("Update error: %v", err)
	}

	// Get by conditions
	foundUsers, err := users.GetBy(map[string]interface{}{"age": 31}).Exec(ctx)
	if err != nil {
		log.Printf("GetBy error: %v", err)
	} else {
		log.Printf("Found users: %+v", foundUsers)
	}

	// Using transaction
	err = users.WriteTransaction().Execute(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.Exec("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)", "2", "Jane Doe", 25)
		if err != nil {
			return err
		}
		_, err = tx.Exec("UPDATE users SET age = $1 WHERE id = $2", 32, "1")
		return err
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Println("All operations completed successfully")
}
