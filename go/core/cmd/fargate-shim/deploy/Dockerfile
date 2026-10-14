FROM scratch
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY fargate-shim /fargate-shim
COPY mockllm /mockllm
ENTRYPOINT ["/fargate-shim"]
