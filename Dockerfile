FROM golang:1.24-alpine

ARG TARGETOS
ARG TARGETARCH

RUN wget https://github.com/hokaccha/spannerdef/releases/download/v1.0.1/spannerdef-v1.0.1-$TARGETOS-$TARGETARCH -O /usr/local/bin/spannerdef
RUN chmod +x /usr/local/bin/spannerdef
