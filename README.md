# Mini-Splunk: Distributed Log Analytics System

A containerized log analytics prototype developed as a **Distributed Systems** academic project. It provides a command-line client and REST API for submitting syslog data, processing messages asynchronously, storing parsed records, and searching collected logs.

The project explores service separation, message queues, container orchestration, and basic failure handling. It is an educational prototype, **not a production monitoring platform or a replacement for Splunk**.

## Features

- **Log ingestion:** Uploads a text file of syslog entries through a Python CLI and an HTTP API.
- **Asynchronous processing:** Publishes individual log lines to a RabbitMQ queue for a separate worker to consume.
- **Syslog parsing:** Extracts timestamps, hostnames, process names, and messages from supported syslog lines.
- **Basic severity classification:** Labels messages as `error`, `warning`, `debug`, or `info` using keyword matching.
- **Persistent record storage:** Writes parsed records to Apache Cassandra within the running containerized environment.
- **Log queries:** Searches by hostname, process, severity, message keyword, or timestamp text; supports keyword counts.
- **Administrative deletion:** Provides a command to clear stored logs.
- **Message handling:** Uses worker acknowledgments on completion and negative acknowledgments with requeuing when processing raises an exception.

## Tech Stack

| Component | Technology | Role |
| --- | --- | --- |
| CLI forwarder | Python, `requests` | Sends log files and query commands to the API |
| API gateway | Python, `aiohttp`, `pika` | Exposes REST endpoints and publishes log lines to RabbitMQ |
| Message broker | RabbitMQ | Buffers log messages for the worker |
| Background worker | Python, `pika`, `cassandra-driver` | Parses and stores log records |
| Data store | Apache Cassandra | Stores syslog records |
| Deployment | Docker, Docker Compose | Runs the containerized services |

**Note:** The Compose file also starts a Redis container, but the supplied Python source code does not use Redis for locking or caching.

## Architecture

```text
                  Syslog text file
                         |
                         v
             Python CLI forwarder
                         |
                    HTTP POST
                         v
                 API Gateway
                  (aiohttp)
                         |
                   Publish logs
                         v
                    RabbitMQ
                   ingest_queue
                         |
                    Consume logs
                         v
                  Python Worker
              (parse + classify)
                         |
                  Insert records
                         v
               Apache Cassandra

For searches and deletion:
CLI --> API Gateway --> Cassandra --> JSON response
```

The API accepts ingestion requests and queues log lines; the worker processes them separately. A successful ingestion response means the lines were **published to the queue**, not necessarily that every line has already been parsed and stored.

## Repository Structure

```text
mini-splunk/
├── api-gateway/
│   ├── app/
│   │   └── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── forwarder/
│   └── cli/
│       └── forwarder_client.py
├── worker/
│   ├── worker/
│   │   └── worker_node.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
└── README.md
```

## Getting Started

### Prerequisites

- Docker with Docker Compose support
- Python 3 for the CLI
- Python package `requests` for the CLI

### 1. Start the services

From the repository's root folder:

```bash
docker compose up --build -d
```

Check the containers and gateway logs:

```bash
docker compose ps
docker compose logs -f api-gateway worker
```

Apache Cassandra can take some time to initialize. The gateway is available on `http://localhost:8080` once it is running.

### 2. Check the API

Open the following address in your browser:

```text
http://localhost:8080/health
```

Expected response:

```json
{"status": "healthy", "service": "api-gateway"}
```

This endpoint confirms that the API responds; it does **not** verify the health of RabbitMQ, Cassandra, or the worker.

### 3. Prepare sample logs

Create a file named `sample.log` in the repository root with the following lines:

```text
Oct 28 13:01:02 lab-host sshd[1821]: error: simulated login failure
Oct 28 13:01:05 lab-host cron[304]: Scheduled job completed
Oct 28 13:02:10 lab-host app[456]: warning: high memory usage
```

The parser expects a syslog-like format containing a timestamp, hostname, process name, and colon-separated message. Not all syslog formats are supported.

### 4. Install the CLI dependency

```bash
python -m pip install requests
```

### 5. Ingest logs

```bash
python forwarder/cli/forwarder_client.py INGEST sample.log localhost:8080
```

The CLI sends the file contents to `POST /ingest`. The gateway publishes each nonempty line as a message to RabbitMQ, and the worker parses and stores supported lines in Cassandra.

### 6. Query stored logs

Allow the background worker a moment to process queued messages, then try:

```bash
# Find logs from a hostname
python forwarder/cli/forwarder_client.py QUERY localhost:8080 SEARCH_HOST lab-host

# Find messages classified as errors
python forwarder/cli/forwarder_client.py QUERY localhost:8080 SEARCH_SEVERITY error

# Search within message text
python forwarder/cli/forwarder_client.py QUERY localhost:8080 SEARCH_KEYWORD login

# Count matching messages
python forwarder/cli/forwarder_client.py QUERY localhost:8080 COUNT_KEYWORD warning
```

Other supported query types are `SEARCH_DAEMON` (process name) and `SEARCH_DATE` (timestamp text). The response contains JSON results, or a count for `COUNT_KEYWORD`.

### 7. Purge records (optional)

**Warning:** This removes all records from the Cassandra `logs` table.

```bash
python forwarder/cli/forwarder_client.py PURGE localhost:8080
```

### 8. Stop the environment

```bash
docker compose down
```

The supplied Compose file does not define a named persistent volume for Cassandra; data retention after recreating or removing containers is therefore not guaranteed.

## REST API

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Check if the API responds |
| `POST` | `/ingest` | Accept JSON containing a `content` string of log lines |
| `GET` | `/query?type=...&value=...` | Search stored logs or count keyword matches |
| `DELETE` | `/purge` | Clear the stored logs table |

## Implementation Notes and Limitations

This repository is an academic prototype with several known limitations:

- **Single Cassandra instance:** The Compose configuration starts one Cassandra container, not a multi-node database cluster.
- **Redis not integrated:** Redis is started by Compose but is not referenced in the current application logic.
- **Application-side filtering:** The query handler selects records from Cassandra and filters them in Python. This is simple but does not scale efficiently for large datasets.
- **Limited syslog formats:** The regex parser supports a specific syslog-like format, and severity classification is based on keywords rather than structured priority fields.
- **Unparseable messages:** The worker logs a warning and acknowledges lines that it cannot parse, so they are not automatically retried or stored.
- **Repeated processing errors:** Exceptions cause messages to be requeued, but there is no configured retry limit or dead-letter queue. Repeated failures can cause repeated delivery attempts.
- **No verified performance claims:** The repository does not include automated benchmark scripts or reproducible evidence for throughput, fault-tolerance, or zero-data-loss claims.
- **Development-only configuration:** RabbitMQ's default credentials and exposed service ports are not suitable for deployment in an untrusted environment without additional security controls.

## Possible Improvements

- Add unit and integration tests for parsing, ingestion, and querying.
- Push supported queries down to Cassandra rather than scanning all rows in Python.
- Add bounded retry policies and a dead-letter queue for failed messages.
- Improve readiness checks and reconnect behavior across services.
- Add configurable credentials, secrets management, and persistent database volumes.
- Measure ingestion throughput and processing behavior with repeatable benchmarks.

## Project Context

**Course:** Distributed Systems  
**Year:** 2026  
**Type:** Academic project

This project was built to explore the design and integration of multiple services involved in log ingestion and analysis. Its main focus is the end-to-end flow between the CLI, REST API, message broker, worker, and database.
