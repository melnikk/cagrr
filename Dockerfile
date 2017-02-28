FROM golang:latest

RUN mkdir /cagrr
ADD . /cagrr/
WORKDIR /cagrr
RUN go get github.com/kardianos/govendor && govendor sync
RUN go build -o main .
CMD ["/cagrr/main"]

EXPOSE 6060
EXPOSE 8888
