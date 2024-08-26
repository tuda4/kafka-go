# Kafka go basic

Pull kafka images

```
docker pull apache/kafka:latest
```

Run Kafka container

```
docker run -p 9092:9092 apache:kafka:latest
```

Run both producer and worker service

```
cd producer / worker
go run main.go
```

Test

```
curl -X POST http://localhost:8080/order \
    -H "Content-Type: application/json" \
    -d '{
        "type": "White Coffe",
        "name": "Guest 1"
    }'
```
