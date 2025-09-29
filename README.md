# gecko

gecko is a configuration server used for fetching inserting user specified configurations that are set during etl jobs or frontend actions.

## local setup

Make sure the below command matches whatever was specified in the init db script. For local, going to need to disable sslmode. Ex:

```
./init_postgres.sh
go build -o bin/gecko
kubectl port-forward svc/local-qdrant 6334:6334
./bin/gecko -db "postgresql://postgres:your_strong_password@localhost:5432/testdb?sslmode=disable" -port 8080 -qdrant-api-key "YOUR_API_KEY_GOES_HERE" -qdrant-host localhost -qdrant-port 6334
go test -v ./...
```

## helm cluster setup

See helm charts for cluster setup.

## starting integration with Qdrant
