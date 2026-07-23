# Build a static tickstore binary, then ship it on a minimal distroless image.
FROM golang:1.26 AS build
WORKDIR /src

# Cache dependencies separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /tickstore ./cmd/tickstore

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /tickstore /tickstore
# Config is mounted at runtime by docker-compose.
ENTRYPOINT ["/tickstore"]
CMD ["-config", "/etc/tickstore/config.yaml"]
