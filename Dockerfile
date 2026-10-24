FROM golang:1.23-alpine AS builder

WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/beacon .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/beacon /beacon
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/beacon"]
CMD ["api"]
