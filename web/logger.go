package web

import (
	"fmt"
	"log"
	"github.com/AntipovVlad/key-value-storage/storage"
	"github.com/AntipovVlad/key-value-storage/transaction_logger"
)

var logger transactionlogger.TransactionLogger

func initializeTransactionLog() error {
	var err error

	events, errors := logger.ReadEvents()
	e, ok := transactionlogger.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <- events:
			switch e.EventType {
			case transactionlogger.EventDelete:
				log.Println("Del")
				err = storage.Delete(e.Key)
			case transactionlogger.EventPut:
				log.Println("Put")
				err = storage.Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}

func InitializeFileTransactionLog() error {
	var err error

	logger, err = transactionlogger.NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	return initializeTransactionLog()
}

func InitializePsqlTransactionLog(config transactionlogger.PostgreDBParams) error {
	var err error

	logger, err = transactionlogger.NewPostgresTransactionLogger(config)
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	return initializeTransactionLog()
}

func FinishTransactionLog() {
	logger.Close()
}