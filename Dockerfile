FROM calavera/go-glide:v0.12.2

ADD . /go/src/github.com/netlify/elastinats

RUN useradd -m netlify && \
        cd /go/src/github.com/netlify/elastinats && \
        make deps build && \
        mv elastinats  /usr/local/bin/

USER netlify
CMD ["elastinats"]
