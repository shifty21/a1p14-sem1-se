FROM scratch

MAINTAINER ykhedar (yateender.khedar@yateenderk@gmail.com)

ADD /bin/gserve /gserve

#build with CGO_ENABLED=0 GOOS=linux GOARCH=amd64
ENTRYPOINT ["./gserve"]
