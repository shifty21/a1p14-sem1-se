GOPATH := ${PWD}:${GOPATH}
export GOPATH

default: build

build:
	env GOOS=linux GOARCH=amd64 go build -tags netgo -v -o ./bin/gserve ./src/gserve 
