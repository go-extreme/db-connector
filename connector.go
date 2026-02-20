package dbconnector

import (
	"context"
	"sync"
)

type Connector interface {
	Read() Connection
	Write() Connection
	Connect(ctx context.Context) error
	Close() error
}

type CQRSConnector struct {
	read  Connection
	write Connection
}

func NewConnector(readConn, writeConn Connection) *CQRSConnector {
	return &CQRSConnector{
		read:  readConn,
		write: writeConn,
	}
}

func (c *CQRSConnector) Read() Connection {
	return c.read
}

func (c *CQRSConnector) Write() Connection {
	return c.write
}

func (c *CQRSConnector) Connect(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.read.Connect(ctx); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := c.write.Connect(ctx); err != nil {
			errChan <- err
		}
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CQRSConnector) Close() error {
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.read.Close(); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := c.write.Close(); err != nil {
			errChan <- err
		}
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}
