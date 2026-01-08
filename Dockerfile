FROM golang:1.25-bookworm as builder

WORKDIR /build

COPY . .
RUN go mod download

RUN go build -o /build/nationpulse-data-ingestion-svc ./cmd

FROM scratch

WORKDIR /app

COPY --from=builder /build/nationpulse-data-ingestion-svc /app

EXPOSE 8083

ENTRYPOINT [ "/app" ]