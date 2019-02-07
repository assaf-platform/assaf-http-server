FROM golang:latest as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build

ENV GOPATH /build
ENV GOBIN /build

RUN go get
RUN go get github.com/stretchr/testify/assert

RUN go test

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o wonder-router .

FROM scratch

COPY --from=builder /build/wonder-router /app/
WORKDIR /app

ENTRYPOINT ["./wonder-router"]
