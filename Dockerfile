FROM golang:alpine
RUN apk update && \
    apk upgrade && \
    apk add git
ENV CAGRR_HOME=/go/src/github.com/skbkontur/cagrr
RUN mkdir -p $CAGRR_HOME
ADD . $CAGRR_HOME
WORKDIR $CAGRR_HOME
RUN go get github.com/kardianos/govendor && govendor sync
RUN go build -o main .
CMD ["./main"]

EXPOSE 6060
EXPOSE 8888
