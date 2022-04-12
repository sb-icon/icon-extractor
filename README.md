<p align="center">
  <h2 align="center">ICON Extractor</h2>
</p>


Icon Extractor lets you convert ICON blockchain data into a single kafka topic.

### Quickstart
```bash
make up
```

### Example Config
```docker-compose
version: "3.7"

x-extractor-env: &extractor-env
  # Extractors
  START_HEAD_EXTRACTOR: "true"                  # Head extractor (Live blocks)
  HEAD_EXTRACTOR_START_BLOCK: "40000000"        # Starting point for head extractor, will continue to wait for future blocks
  START_CLAIM_EXTRACTORS: "true"                # Claim extractor (Historical blocks)
  NUM_CLAIM_EXTRACTORS: "4"                     # Number of claim extractors to start (go routines)

  # Kakfa
  KAFKA_BROKER_URL: "kafka:9092"

  # DB
  DB_DRIVER: "postgres"
  DB_HOST: "postgres"
  DB_PORT: "5432"
  DB_USER: "postgres"
  DB_PASSWORD: "changeme"
  DB_DBNAME: "postgres"
  DB_SSL_MODE: "disable"
  DB_TIMEZONE: "UTC"

services:
  extractor:
    image: sudoblock/icon-extractor:latest
    ports:
      - "8000:8000"
    environment:
      <<: *extractor-env
```

#### Head Extractor
To use the head extractor, 2 enviroment variables need to be set. 
```
START_HEAD_EXTRACTOR: "true"
HEAD_EXTRACTOR_START_BLOCK: "40000000"
```
The head extractor will start at the `HEAD_EXTRACTOR_START_BLOCK` and will continue forever. The head extractor will wait if a block has not been created yet.

#### Claim Extractor
To use the claim extractors, 2 enviroment variables need to be set.
```
START_CLAIM_EXTRACTORS: "true"
NUM_CLAIM_EXTRACTORS: "4"
```
The claim extractors will be run on go routines. There will be `NUM_CLAIM_EXTRACTORS` go routines. The claim extractor will ping the postgres database for claims, located in the claims table. To create claims, visit `http://localhost:8000/api/v1/docs` for Swagger Docs.
>Example curl command:
```
curl -X 'POST' \
  'http://localhost:8000/api/v1/create-job' \
  -H 'accept: application/json' \
  -H 'Content-Type: */*' \
  -d '{
  "end_block_number": 3000,
  "start_block_number": 1
}'
```

### Internal diagram![Blank diagram](https://user-images.githubusercontent.com/77865393/162858201-1eeda5b5-8134-4c87-93c3-d2e1dc6f88a3.png)
