# Spanner storage backend for Jaeger

**Note:** this plugin is an experiment. Meant to learn more about Jaeger and how it works. It was written during a "lab day" and is by no means production-ready. 

## Development

Download Jaeger "all-in-one" binary: https://www.jaegertracing.io/download/

Setup and start a [Spanner emulator](https://cloud.google.com/spanner/docs/emulator): `gcloud emulators spanner start`

Create a Spanner instance: `gcloud spanner instances create test-instance --config=emulator-config --description="Test Instance" --nodes=1`

Frontend URL: http://localhost:16686/search

Run: `make dev`

## Useful links

* [gRPC storage plugins for Jaeger](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc)

## License

[Apache 2.0 License](./LICENSE).