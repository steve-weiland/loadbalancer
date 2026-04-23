FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o bin/lbserver    ./cmd/lbserver \
 && go build -o bin/echobackend ./cmd/echobackend

FROM alpine:3.19
COPY --from=builder /app/bin/lbserver    /lbserver
COPY --from=builder /app/bin/echobackend /echobackend
EXPOSE 7080 7090
# No ENTRYPOINT — docker-compose chooses per service.
