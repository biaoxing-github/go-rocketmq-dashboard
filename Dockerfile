ARG GO_VERSION=1.25
ARG ROCKETMQ_VERSION=5.3.2
ARG ROCKETMQ_DOWNLOAD_BASE=https://mirrors.huaweicloud.com/apache/rocketmq
ARG ROCKETMQ_CHECKSUM_BASE=https://downloads.apache.org/rocketmq
ARG GO_IMAGE=golang:${GO_VERSION}-bookworm
ARG JAVA_IMAGE=eclipse-temurin:17-jdk-jammy

FROM ${GO_IMAGE} AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/rmqdash ./cmd/rmqdash
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/goadmin ./cmd/goadmin

FROM ${JAVA_IMAGE} AS runtime

ARG ROCKETMQ_VERSION
ARG ROCKETMQ_DOWNLOAD_BASE
ARG ROCKETMQ_CHECKSUM_BASE
ARG APT_MIRROR=https://repo.huaweicloud.com/ubuntu/

RUN set -eux; \
    if [ -f /etc/apt/sources.list ]; then \
        sed -i "s#http://archive.ubuntu.com/ubuntu/#${APT_MIRROR}#g; s#http://security.ubuntu.com/ubuntu/#${APT_MIRROR}#g" /etc/apt/sources.list; \
    fi; \
    if [ -d /etc/apt/sources.list.d ]; then \
        find /etc/apt/sources.list.d -type f -name '*.sources' -print0 | xargs -0 -r sed -i "s#http://archive.ubuntu.com/ubuntu/#${APT_MIRROR}#g; s#http://security.ubuntu.com/ubuntu/#${APT_MIRROR}#g"; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl unzip; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    file="rocketmq-all-${ROCKETMQ_VERSION}-bin-release.zip"; \
    curl -fSL --retry 5 --retry-delay 5 --retry-all-errors -C - "${ROCKETMQ_DOWNLOAD_BASE}/${ROCKETMQ_VERSION}/${file}" -o "/tmp/${file}"; \
    if [ -n "${ROCKETMQ_CHECKSUM_BASE}" ]; then \
        curl -fSL --retry 5 --retry-delay 5 --retry-all-errors "${ROCKETMQ_CHECKSUM_BASE}/${ROCKETMQ_VERSION}/${file}.sha512" -o "/tmp/${file}.sha512"; \
        expected="$(grep -Eo '[A-Fa-f0-9]{8}' "/tmp/${file}.sha512" | tr -d '\n' | tr '[:lower:]' '[:upper:]')"; \
        actual="$(sha512sum "/tmp/${file}" | awk '{print toupper($1)}')"; \
        test -n "${expected}"; \
        test "${actual}" = "${expected}"; \
    fi; \
    unzip -q "/tmp/${file}" -d /opt; \
    mv "/opt/rocketmq-all-${ROCKETMQ_VERSION}-bin-release" /opt/rocketmq; \
    rm -f "/tmp/${file}" "/tmp/${file}.sha512"; \
    test -d /opt/rocketmq/lib

COPY cmd/rocketmq-admin-sidecar/AdminSidecar.java /tmp/AdminSidecar.java
RUN set -eux; \
    mkdir -p /app/rocketmq-admin-sidecar; \
    javac -encoding UTF-8 -cp "/opt/rocketmq/lib/*" -d /app/rocketmq-admin-sidecar /tmp/AdminSidecar.java; \
    rm -f /tmp/AdminSidecar.java

RUN set -eux; \
    groupadd --system rmqdash; \
    useradd --system --gid rmqdash --home-dir /app --shell /usr/sbin/nologin rmqdash; \
    mkdir -p /app; \
    chown -R rmqdash:rmqdash /app

COPY --from=builder /out/rmqdash /usr/local/bin/rmqdash
COPY --from=builder /out/goadmin /usr/local/bin/goadmin

WORKDIR /app

ENV HOME=/app \
    ROCKETMQ_HOME=/opt/rocketmq \
    RMQD_ADDR=:18090 \
    RMQD_JAVA=java \
    RMQD_MQADMIN_CLASSPATH=/opt/rocketmq/lib/* \
    RMQD_ROCKETMQ_VERSION=${ROCKETMQ_VERSION} \
    RMQD_REQUEST_TIMEOUT_MS=60000 \
    RMQD_CLUSTER_CACHE_TTL_MS=30000 \
    RMQD_MESSAGE_CHAIN_CACHE_TTL_MS=1800000 \
    RMQD_COMMAND_MAX_LATENCY_MS=1000 \
    RMQD_ADMIN_SIDECAR_ENABLED=true \
    RMQD_ADMIN_SIDECAR_ADDR=127.0.0.1:18091 \
    RMQD_ADMIN_SIDECAR_CLASSPATH=/app/rocketmq-admin-sidecar:/opt/rocketmq/lib/* \
    RMQD_ADMIN_SIDECAR_MAIN_CLASS=dev.codex.rocketmq.AdminSidecar \
    RMQD_ADMIN_SIDECAR_TIMEOUT_MS=3000

EXPOSE 18090

USER rmqdash

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:18090/api/health >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/rmqdash"]
