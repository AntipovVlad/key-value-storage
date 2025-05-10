package transactionlogger

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key string, value string)
	Close()

	Err() <- chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()
}

type FileTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	wg sync.WaitGroup
	lastSequence uint64
	file *os.File
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	hexkey := hex.EncodeToString([]byte(key))
	l.events <- Event{EventType: EventDelete, Key: hexkey}
}

func (l *FileTransactionLogger) WritePut(key string, value string) {
	hexkey := hex.EncodeToString([]byte(key))
	hexvalue := hex.EncodeToString([]byte(value))
	l.events <- Event{EventType: EventPut, Key: hexkey, Value: hexvalue}
}

func (l *FileTransactionLogger) Close() {
	l.wg.Wait()
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	l.wg.Add(1)

	go func () {
		defer l.wg.Done()
		defer l.file.Close()

		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)
			
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	// Restore storage from file
	
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event
		var matched error

		defer close(outEvent)
		defer close(outError)

		patterns := []string{"%d\t%d\t%s\t%s", "%d\t%d\t%s"}

		for scanner.Scan() {
			line := scanner.Text()
			
			for _, pattern := range patterns {
				if _, err := fmt.Sscanf(line, pattern, 
						&e.Sequence, &e.EventType, &e.Key, &e.Value); err == nil {
					matched = err
					break
				}
			}

			if matched != nil {
				outError <- fmt.Errorf("input parse error: %w", matched)
				return
			}

			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			l.lastSequence = e.Sequence

			key, _ := hex.DecodeString(e.Key)
			e.Key = string(key)
			value, _ := hex.DecodeString(e.Value)
			if len(value) != 0 {
				e.Value = string(value)
			}

			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err) 
			return
		}
	}()

	return outEvent, outError
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &FileTransactionLogger{file: file}, nil
}
