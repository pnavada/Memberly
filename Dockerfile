FROM ubuntu:22.04

RUN apt-get update && apt install -y golang

WORKDIR /home/ubuntu

COPY peer main.go hostsfile.txt /home/ubuntu/

ENV GOPATH=/go

RUN go mod init peer && go mod tidy

ENTRYPOINT [ "go", "run", "main.go" ]