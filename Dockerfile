FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY bin/etcd-steward /etcd-steward
ENTRYPOINT ["/etcd-steward"]
