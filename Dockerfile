FROM golang:1.16-buster AS builder

# GO ENV VARS
ENV GO111MODULE=on \
    CGO_ENABLED=1 \
    GOOS=linux \
    GOARCH=amd64

# COPY SRC
WORKDIR /build
COPY ./src .

RUN go mod tidy

# BUILD
WORKDIR /build
RUN go build -o main ./

FROM ubuntu as prod
RUN apt update && apt install curl -y

# For SSL certs
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

RUN useradd -ms /bin/bash icon
USER icon
WORKDIR /home/icon

COPY --from=builder /build/main ./
CMD ["./main"]
