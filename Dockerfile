FROM ubuntu:22.04

RUN apt-get update && apt install -y golang

WORKDIR /home/ubuntu

COPY peer.go .
COPY hostsfile.txt .

ENTRYPOINT [ "go", "run", "peer.go" ]