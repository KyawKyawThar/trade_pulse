FROM golang:1.26.1-alpine AS build

# Copy only the modules this service needs, deps first for layer caching.

COPY shared/ ./shared
COPY services/analytics-service/go.mod services/analytics-service/go.sum* ./services/analytics-service/
WORKDIR /src/services/analytics-service
RUN go mod download

# Now the source, then build a static binary (no cgo for the skeleton).
COPY services/analytics-service/ ./
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