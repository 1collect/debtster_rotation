FROM golang:1.23-alpine AS build

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /rotation-go ./cmd/server

FROM alpine:3.20

WORKDIR /app
COPY --from=build /rotation-go /app/rotation-go

EXPOSE 8040
CMD ["/app/rotation-go"]
