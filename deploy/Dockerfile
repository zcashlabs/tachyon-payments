# Multi-stage packaging matching zecstack's Pir.Dockerfile:
# Caddy TLS edge (capabilities stripped for non-root k8s) + release PIR binaries.
FROM caddy:2.10.2-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d AS proxy
RUN setcap -r /usr/bin/caddy

FROM ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libstdc++6 curl \
    && rm -rf /var/lib/apt/lists/*
COPY target/release/tachyon-pir-publisher /usr/local/bin/tachyon-pir-publisher
COPY target/release/tachyon-pir-query-server /usr/local/bin/tachyon-pir-query-server
COPY target/release/tachyon-pir-cli /usr/local/bin/tachyon-pir-cli
COPY --from=proxy /usr/bin/caddy /usr/local/bin/caddy
USER 10001:10001
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/tachyon-pir-query-server"]
