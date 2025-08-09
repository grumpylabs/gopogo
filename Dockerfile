FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN make build

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /build/bin/gopogo /app/gopogo

EXPOSE 6379 8080 11211 5432

ENTRYPOINT ["/app/gopogo"]
CMD ["-h", "0.0.0.0"]