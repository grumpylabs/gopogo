# Packages the static binary built by `make amd64` / `make arm64`; see the
# image targets in the Makefile.
FROM alpine:latest

ARG TARGETARCH

LABEL org.opencontainers.image.source="https://github.com/grumpylabs/gopogo"
LABEL org.opencontainers.image.description="Gopogo multi-protocol cache server"

WORKDIR /app

COPY bin/gopogo-${TARGETARCH} /app/gopogo

# Run as a non-root user; the Helm chart uses the same IDs.
USER 65532:65532

EXPOSE 6379 8080 11211 5432

ENTRYPOINT ["/app/gopogo"]
CMD ["--host", "0.0.0.0"]
