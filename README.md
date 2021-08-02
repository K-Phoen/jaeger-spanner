# Spanner storage backend for Jaeger

## Dev setup

Download Jaeger "all-in-one" binary: https://www.jaegertracing.io/download/

Setup and start a [Spanner emulator](https://cloud.google.com/spanner/docs/emulator): `gcloud emulators spanner start`

Create a Spanner instance: `gcloud spanner instances create test-instance --config=emulator-config --description="Test Instance" --nodes=1`

Frontend URL: http://localhost:16686/search

Run: `make dev`

## Useful links

* [gRPC storage plugins for Jaeger](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc)

## License

[Apache 2.0 License](./LICENSE).