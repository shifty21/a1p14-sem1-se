FROM scratch

MAINTAINER ykhedar (yateender.khedar@yateenderk@gmail.com)

ADD /bin/grproxy /grproxy

#build with CGO_ENABLED=0 GOOS=linux GOARCH=amd64
ENTRYPOINT ["./grproxy"]

EXPOSE 8080
