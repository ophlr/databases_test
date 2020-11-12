FROM golang:1.15.2-alpine

RUN apk update && apk add build-base

COPY . /data/app/databases_test
WORKDIR /data/app/databases_test

RUN go mod vendor

CMD ./run.sh
#CMD ["tail", "-f", "/dev/null"]
