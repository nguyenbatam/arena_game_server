FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/loadtest ./cmd/loadtest && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/replay ./cmd/replay

FROM alpine:3.24.1
WORKDIR /app
RUN adduser -D arena
# replay ships with the image on purpose: the recordings are written inside the
# container, and a verifier you cannot run where the files are is half a tool.
COPY --from=build /out/server /out/loadtest /out/replay ./
COPY web ./web
ENV WEB_DIR=./web
USER arena
EXPOSE 8080 8081
ENTRYPOINT ["./server"]
