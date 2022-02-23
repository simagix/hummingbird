FROM golang:1.16 as builder
RUN mkdir -p /go/src/github.com/simagix/hummingbird
ADD . /go/src/github.com/simagix/hummingbird
WORKDIR /go/src/github.com/simagix/hummingbird
RUN ./build.sh
FROM alpine
LABEL Ken Chen <ken.chen@simagix.com>
RUN addgroup -S mongo && adduser -S mongo -G mongo
USER mongo
COPY --from=builder /go/src/github.com/simagix/hummingbird/dist/neutrino-* /dist/
WORKDIR /
CMD ["/dist/neutrino-linux-x64", "--version"]