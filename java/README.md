# kyuubi-batch-submitter

Java library for submitting and monitoring [Apache Kyuubi](https://kyuubi.apache.org/) batch jobs via the REST API.

Designed to run **within a Spark 3.5.x environment** — Jackson and Apache HttpClient are treated as `provided` dependencies (already on the Spark classpath). The only bundled dependency is `httpmime` for multipart file uploads.

---

## Requirements

| Dependency | Version |
|---|---|
| JDK | 17+ |
| Spark | 3.5.6 (httpclient 4.5.x, jackson 2.15.x on classpath) |
| Kyuubi | 1.x (REST API v1) |

---

## Build

```bash
mvn package -DskipTests
```

This produces two JARs under `target/`:

| Artifact | Description |
|---|---|
| `kyuubi-batch-submitter-1.0.0.jar` | Thin JAR (no deps — use when running inside Spark) |
| `kyuubi-batch-submitter-1.0.0-shaded.jar` | Fat JAR including httpmime (for standalone use) |

---

## Quick Start

```java
import com.nx1.kyuubi.KyuubiClient;
import com.nx1.kyuubi.model.*;

KyuubiClientConfig cfg = KyuubiClientConfig.builder()
    .serverUrl("https://kyuubi.internal:10099")
    .username("svc-spark")
    .password(System.getenv("KYUUBI_PASSWORD"))
    .historyServerUrl("http://spark-history:18080") // optional
    .pollIntervalMs(15_000)
    .build();

SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://my-bucket/jobs/etl.py")        // remote URI — no upload
    .name("daily-etl")
    .conf("spark.executor.memory", "4g")
    .conf("spark.executor.cores", "2")
    .arg("--date").arg("2024-01-15")
    .build();

try (KyuubiClient client = new KyuubiClient(cfg)) {
    // Submit and wait, streaming logs to stdout
    BatchState state = client.submitAndWait(opts, System.out::println);

    if (state != BatchState.FINISHED) {
        throw new RuntimeException("Job did not succeed: " + state);
    }
}
```

---

## Local File Upload

If you pass a **local filesystem path** as `resource`, `pyFile`, `jar`, or `file`, the library automatically detects it and switches to a multipart upload. Remote URIs (`s3a://`, `hdfs://`, `gs://`, etc.) are passed through untouched.

```java
SubmitOptions opts = SubmitOptions.builder()
    .resource("/opt/spark/jobs/etl.py")           // LOCAL → uploaded via multipart
    .pyFile("/opt/spark/lib/utils.zip")           // LOCAL → uploaded
    .jar("s3a://bucket/lib/connector.jar")        // REMOTE → passed in conf
    .name("etl-with-local-files")
    .build();
```

---

## Fire-and-Forget vs. Blocking

```java
// Fire-and-forget: returns immediately with the batch ID
String batchId = client.submit(opts);

// Poll status manually
BatchStatus status = client.getStatus(batchId);

// Block until terminal state
BatchState state = client.monitor(batchId);

// Block + stream logs
BatchState state = client.monitor(batchId, line -> myLogger.info("[spark] {}", line));

// Cancel
client.cancel(batchId);
```

---

## YuniKorn Queue Routing

Set the YuniKorn placement labels via Spark conf:

```java
SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://bucket/job.py")
    .name("etl")
    .conf("spark.kubernetes.driver.label.queue",   "root.default.analytics")
    .conf("spark.kubernetes.executor.label.queue", "root.default.analytics")
    .build();
```

---

## Configuration Reference

### `KyuubiClientConfig`

| Method | Default | Description |
|---|---|---|
| `serverUrl(String)` | required | Kyuubi REST base URL |
| `username(String)` | required | Basic-auth username |
| `password(String)` | required | Basic-auth password |
| `historyServerUrl(String)` | null | Spark History Server URL |
| `pollIntervalMs(long)` | 10 000 | Status poll interval |
| `connectTimeoutMs(int)` | 30 000 | HTTP connect timeout |
| `socketTimeoutMs(int)` | 60 000 | HTTP read timeout |
| `disableSslVerification()` | false | Skip TLS cert check (**dev only**) |

### `SubmitOptions`

| Method | Description |
|---|---|
| `resource(String)` | Main JAR or Python file (local or remote) |
| `className(String)` | Main class for JAR jobs (ignored for PySpark) |
| `name(String)` | Display name |
| `arg(String)` / `args(List)` | Positional arguments |
| `conf(String, String)` / `conf(Map)` | Spark configuration |
| `pyFile(String)` / `pyFiles(List)` | Python deps (local or remote) |
| `jar(String)` / `jars(List)` | JAR deps (local or remote) |
| `file(String)` / `files(List)` | Files to distribute (local or remote) |

---

## Integration Test

`KyuubiClientIT` submits a real PySpark job to a live Kyuubi instance. It is skipped by default and only runs when the required environment variables are set.

```bash
cd java
KYUUBI_SERVER_URL=https://kyuubi.example.com \
KYUUBI_USERNAME=your-username \
KYUUBI_PASSWORD=your-password \
mvn verify -Dit.test=KyuubiClientIT
```
