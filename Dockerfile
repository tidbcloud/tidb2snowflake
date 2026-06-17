FROM ubuntu:latest

RUN apt-get update && \
    apt-get -y --no-install-recommends install ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    update-ca-certificates

COPY bin/tidb2snowflake-linux-amd64 /bin/tidb2snowflake

ENTRYPOINT ["/bin/tidb2snowflake"]
