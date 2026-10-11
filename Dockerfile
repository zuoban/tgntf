# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/tgntf ./cmd/tgntf

FROM alpine:3.23
RUN apk add --no-cache ca-certificates \
    && addgroup -g 10001 app \
    && adduser -D -u 10001 -G app app \
    && mkdir /data && chown app:app /data
COPY --from=build /out/tgntf /usr/local/bin/tgntf
USER 10001:10001
ENV STATE_PATH=/data/notifications.db HEALTH_ADDR=127.0.0.1:8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/tgntf", "-healthcheck"]
ENTRYPOINT ["/usr/local/bin/tgntf"]
