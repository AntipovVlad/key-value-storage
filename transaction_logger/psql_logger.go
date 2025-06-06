package transactionlogger

import (
	"database/sql"
	_ "github.com/lib/pq"
	
	"fmt"
	"sync"
)

type PostgreDBParams struct {
	dbName string
	host string
	user string
	password string
}

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db *sql.DB
	wg *sync.WaitGroup
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) WritePut(key string, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) Err() <-chan error { 
	return l.errors
}

func (l *PostgresTransactionLogger) Close() {
	l.wg.Wait()
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transaction"

	var result string

	rows, err := l.db.Query("SELECT to_regclass('public.?');", table)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() && result != table {
		rows.Scan(&result)
	}

	return result == table, rows.Err()
}

func (l *PostgresTransactionLogger) createTable() error {
	var err error

	createQuery := `
		create table transactions(
			sequence      BIGSERIAL PRIMARY KEY,
			event_type    SMALLINT,
			key 		  TEXT,
			value         TEXT
	)`
	
	_, err = l.db.Exec(createQuery)

	return err
}

func NewPostgresTransactionLogger(config PostgreDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s", 
		config.host, config.dbName, config.user, config.password)
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err) 
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err) 
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	l.wg.Add(1)

	go func () {
		defer l.wg.Done()

		insertQuery := `
			insert into transactions
			(event_type, key, value)
			values ()
		`

		for e := range events {
			_, err := l.db.Exec(
				insertQuery,
				e.EventType, e.Key, e.Value)
			
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outError)
		defer close(outEvent)

		query := `select sequence, event_type, key, value
				  from transactions 
				  order by sequence`

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close()

		e := Event{}

		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Key)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err) 
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}
