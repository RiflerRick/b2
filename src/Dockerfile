FROM golang:latest

RUN apt update

RUN go get github.com/go-sql-driver/mysql
RUN go get github.com/golang/glog

WORKDIR /go/app
COPY . /go/app

# not sure if building in the dockerfile is supposed to be done or not
RUN go build -o bombard . 