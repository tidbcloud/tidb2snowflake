FROM ubuntu:latest

RUN apt-get update && \
    apt-get -y --no-install-recommends install ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    update-ca-certificates && \
    groupadd --system tidb2snowflake && \
    useradd --system --gid tidb2snowflake --home-dir /nonexistent --shell /usr/sbin/nologin --no-create-home tidb2snowflake

COPY bin/tidb2snowflake-linux-amd64 /bin/tidb2snowflake
RUN chmod 0755 /bin/tidb2snowflake

USER tidb2snowflake
ENTRYPOINT ["/bin/tidb2snowflake"]
