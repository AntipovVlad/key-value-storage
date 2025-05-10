package main

import (
	"log"
	"net/http"
	"github.com/AntipovVlad/key-value-storage/web"
)

func main() {
	web.InitializeFileTransactionLog()
	web.LinkRoutes()

	err := http.ListenAndServe(":8080", nil)

	web.FinishTransactionLog()

	log.Fatal(err)
}