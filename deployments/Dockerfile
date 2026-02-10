FROM golang:1.23-alpine AS builder

ARG CMD=jupitor-server

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app ./cmd/${CMD}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app /usr/local/bin/app
COPY config/ /etc/jupitor/
ENTRYPOINT ["/usr/local/bin/app"]
