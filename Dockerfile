# syntax=docker/dockerfile:1

# 1) build stage
FROM golang:1.22 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o marketflow ./cmd/marketflow

# 2) runtime stage — используем bookworm для более новой glibc
FROM debian:bookworm-slim

WORKDIR /app

# копируем бинарник
COPY --from=builder /app/marketflow /app/marketflow

# (опционально) копируем init.sql, если хочешь, чтобы контейнер мог обращаться к нему
# COPY init.sql /app/init.sql

# минимально необходимые утилиты (если нужно)
RUN apt-get update \
 && apt-get install -y ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

EXPOSE 8080

CMD ["./marketflow", "--port", "8080"]
