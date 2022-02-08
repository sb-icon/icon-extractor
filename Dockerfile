FROM golang:1.16-buster AS builder

ARG SERVICE_NAME
ENV SERVICE_NAME ${SERVICE_NAME:-api}

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
RUN go build -o main ./${SERVICE_NAME}

FROM ubuntu as prod
COPY --from=builder /build/main /
CMD ["/main"]
