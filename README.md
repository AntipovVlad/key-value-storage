# Key-Value storage
Простое Key-Value хранилище с REST API на базе Golang.

## API
### 1. Добавить значение по ключц

* Endpoint: PUT /v1/key/{key}
* Query Parameters:
    1. key (string, required): Key
* Data: value (string, required)

**Request**
```
PUT /v1/key/apple HTTP/1.1
Host: yourhost.com
Data: juice
```

**Response**
```
HTTP/1.1 201 Created
```

### 2. Получить значение по ключу

* Endpoint: GET /v1/key/{key}
* Query Parameters:
    1. key (string, required): Key

**Request**
```
GET /v1/key/apple HTTP/1.1
Host: yourhost.com
```

**Response**
```
HTTP/1.1 200 OK
juice
```

### 3. Удалить значение по ключу

* Endpoint: DELETE /v1/key/{key}
* Query Parameters:
    1. key (string, required): Key

**Request**
```
DELETE /v1/key/apple HTTP/1.1
Host: yourhost.com
```

**Response**
```
HTTP/1.1 204 No Content
```

## Хранение данных
Хранилище работает in-memory, сохраняя все транзакции в файле для логов или базе Postgres. При запуске оно восстанавливает состояние на основе транзакций.
