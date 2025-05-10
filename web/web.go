package web

import (
	"errors"
	"io"
	"net/http"

	"github.com/AntipovVlad/key-value-storage/storage"
)

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = storage.Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := storage.Get(key)
	if errors.Is(err, storage.ErrorNoSuchKey) {
		http.Error(w,err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError) 
		return
	}

	w.Write([]byte(value))
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	err := storage.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError) 
		return
	}

	logger.WriteDelete(key)

	w.WriteHeader(http.StatusNoContent)
}

func LinkRoutes() {
	http.HandleFunc("PUT /v1/key/{key}", keyValuePutHandler)
	http.HandleFunc("GET /v1/key/{key}", keyValueGetHandler)
	http.HandleFunc("DELETE /v1/key/{key}", keyValueDeleteHandler)
}