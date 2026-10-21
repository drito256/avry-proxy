# multi-stage build for avry-proxy and backend-worker
FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/avry-proxy ./cmd/proxy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/backend-worker ./cmd/worker

FROM alpine:3.20
RUN apk --no-cache add ca-certificates curl
WORKDIR /app
COPY --from=builder /out/avry-proxy /usr/local/bin/avry-proxy
COPY --from=builder /out/backend-worker /usr/local/bin/backend-worker

EXPOSE 8080 8081
ENTRYPOINT ["/usr/local/bin/avry-proxy"]
