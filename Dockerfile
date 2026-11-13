FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/app && CGO_ENABLED=0 go build -o /out/dashboard ./cmd/dashboard
FROM alpine:3.21
RUN adduser -D app
COPY --from=build /out/app /usr/local/bin/app
COPY --from=build /out/dashboard /usr/local/bin/dashboard
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/app"]
