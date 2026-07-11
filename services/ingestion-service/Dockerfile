FROM golang:1.26.5-alpine AS build

WORKDIR /src

# Copy only the modules this service needs, deps first for layer caching.

COPY shared/ ./shared
COPY services/ingestion-service/go.mod services/ingestion-service/go.sum* ./services/ingestion-service/
WORKDIR /src/services/ingestion-service
RUN go mod download

# Now the source, then build a static binary (no cgo for the skeleton).
COPY services/ingestion-service/ ./
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w \
      -X github.com/tradepulse/shared/version.Version=${VERSION} \
      -X github.com/tradepulse/shared/version.Commit=${COMMIT} \
      -X github.com/tradepulse/shared/version.Date=${DATE}" \
   -o /out/service ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT [ "/service" ]