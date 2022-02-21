FROM golang:1.16 as builder
RUN mkdir -p /go/src/github.com/simagix/humingbird
ADD . /go/src/github.com/simagix/humingbird
WORKDIR /go/src/github.com/simagix/humingbird
RUN ./build.sh
FROM alpine
LABEL Ken Chen <ken.chen@simagix.com>
RUN addgroup -S mongo && adduser -S mongo -G mongo
USER mongo
COPY --from=builder /go/src/github.com/simagix/humingbird/dist/neutrino-* /dist/
WORKDIR /
CMD ["/dist/neutrino-linux-x64", "--version"]