# `kyuubi-batch-submitter` — Usage Guide

A Java library for submitting and monitoring [Apache Kyuubi](https://kyuubi.apache.org/) batch jobs via the REST API. Designed to run inside a Spark 3.5.x environment.

---

## Table of Contents

1. [Adding the Dependency](#1-adding-the-dependency)
2. [Building the Client](#2-building-the-client)
3. [Basic: Submit a Remote PySpark Job](#3-basic-submit-a-remote-pyspark-job)
4. [Basic: Submit a Remote JAR Job](#4-basic-remote-jar-job)
5. [Uploading Local Files](#5-uploading-local-files)
6. [Passing Arguments and Spark Conf](#6-passing-arguments-and-spark-conf)
7. [Fire-and-Forget with Manual Polling](#7-fire-and-forget-with-manual-polling)
8. [Streaming Logs](#8-streaming-logs)
9. [Cancellation and Signal Handling](#9-cancellation-and-signal-handling)
10. [YuniKorn Queue Routing](#10-yunikorn-queue-routing)
11. [Using Inside an Airflow PySpark Task](#11-using-inside-an-airflow-pyspark-task)
12. [Error Handling Reference](#12-error-handling-reference)
13. [Full Configuration Reference](#13-full-configuration-reference)

---

## 1. Adding the Dependency

### Maven

Install the JAR to your local Maven repository first:

```bash
mvn install:install-file \
  -Dfile=kyuubi-batch-submitter-1.0.0.jar \
  -DgroupId=com.nx1 \
  -DartifactId=kyuubi-batch-submitter \
  -Dversion=1.0.0 \
  -Dpackaging=jar
```

Then add to your `pom.xml`:

```xml
<dependency>
    <groupId>com.nx1</groupId>
    <artifactId>kyuubi-batch-submitter</artifactId>
    <version>1.0.0</version>
</dependency>
```

> **Note:** When running inside Spark, use the thin JAR (`kyuubi-batch-submitter-1.0.0.jar`).
> Jackson and `httpclient` are already on the Spark classpath. If you need to run standalone
> (e.g., in a plain JVM process), use the shaded JAR (`kyuubi-batch-submitter-1.0.0-shaded.jar`)
> which bundles `httpmime`.

### Gradle

```groovy
dependencies {
    implementation files('libs/kyuubi-batch-submitter-1.0.0.jar')
}
```

### On the Spark `--jars` flag

```bash
spark-submit \
  --jars kyuubi-batch-submitter-1.0.0-shaded.jar \
  --class com.example.MyDriver \
  my-app.jar
```

---

## 2. Building the Client

All configuration goes through `KyuubiClientConfig`. Build it once and reuse the `KyuubiClient`
across multiple submissions — it is thread-safe.

```java
import com.nx1.kyuubi.KyuubiClient;
import com.nx1.kyuubi.model.KyuubiClientConfig;

KyuubiClientConfig config = KyuubiClientConfig.builder()
    .serverUrl("https://kyuubi.internal:10099")       // required
    .username("svc-spark")                            // required
    .password(System.getenv("KYUUBI_PASSWORD"))       // required
    .historyServerUrl("http://spark-history:18080")   // optional, for log URLs
    .pollIntervalMs(15_000)                           // default: 10 000 ms
    .connectTimeoutMs(30_000)                         // default: 30 000 ms
    .socketTimeoutMs(120_000)                         // default: 60 000 ms
    .build();

// KyuubiClient is Closeable — always use try-with-resources
try (KyuubiClient client = new KyuubiClient(config)) {
    // submit jobs here
}
```

To skip TLS certificate verification in dev/test environments:

```java
KyuubiClientConfig config = KyuubiClientConfig.builder()
    .serverUrl("https://kyuubi-dev:10099")
    .username("admin")
    .password("admin")
    .disableSslVerification()   // ⚠️  never use in production
    .build();
```

---

## 3. Basic: Submit a Remote PySpark Job

The simplest case — the script lives in S3, nothing to upload.

```java
import com.nx1.kyuubi.KyuubiClient;
import com.nx1.kyuubi.model.*;

KyuubiClientConfig config = KyuubiClientConfig.builder()
    .serverUrl("https://kyuubi.internal:10099")
    .username("svc-spark")
    .password(System.getenv("KYUUBI_PASSWORD"))
    .build();

SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://nx1-jobs/etl/daily_snapshot.py")
    .name("daily-snapshot")
    .conf("spark.executor.memory", "4g")
    .conf("spark.executor.cores", "2")
    .conf("spark.executor.instances", "5")
    .build();

try (KyuubiClient client = new KyuubiClient(config)) {
    // Blocks until the job reaches a terminal state
    BatchState finalState = client.submitAndWait(opts);

    if (finalState != BatchState.FINISHED) {
        throw new RuntimeException("Job did not succeed. Final state: " + finalState);
    }
    System.out.println("Job completed successfully.");
}
```

The library auto-detects the batch type from the file extension: `.py`, `.zip`, and `.egg`
map to `PYSPARK`; everything else maps to `SPARK`.

---

## 4. Remote JAR Job

For a compiled Spark application JAR hosted in HDFS.

```java
SubmitOptions opts = SubmitOptions.builder()
    .resource("hdfs://namenode:8020/apps/spark/my-etl-1.0.0.jar")
    .className("com.example.etl.DailyAggregator")    // required for JAR jobs
    .name("daily-aggregator")
    .arg("--env").arg("production")
    .arg("--date").arg("2024-03-15")
    .conf("spark.executor.memory", "8g")
    .conf("spark.sql.shuffle.partitions", "200")
    .build();

try (KyuubiClient client = new KyuubiClient(config)) {
    BatchState state = client.submitAndWait(opts);
    System.out.println("Final state: " + state);
}
```

> **className is ignored for PySpark.** You only need it for JAR submissions.

---

## 5. Uploading Local Files

When a path resolves to an existing local file, the library automatically switches to a
multipart upload. You can freely mix local paths and remote URIs in the same submission.

### Local Python script + remote dependency

```java
SubmitOptions opts = SubmitOptions.builder()
    // LOCAL: auto-uploaded via multipart
    .resource("/opt/spark/jobs/transform.py")
    // REMOTE: passed as spark.submit.pyFiles in conf
    .pyFile("s3a://nx1-libs/python/shared_utils-2.1.zip")
    .name("transform-job")
    .conf("spark.executor.memory", "2g")
    .build();
```

### Multiple local dependencies

```java
SubmitOptions opts = SubmitOptions.builder()
    .resource("/opt/spark/jobs/pipeline.py")
    // All three are local → all three get uploaded
    .pyFile("/opt/spark/lib/utils.zip")
    .pyFile("/opt/spark/lib/models.egg")
    .jar("/opt/spark/lib/custom-udf.jar")
    // Config file to distribute to every executor
    .file("/etc/spark/pipeline.conf")
    .name("full-pipeline")
    .build();
```

### Mixed local + remote

```java
SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://nx1-jobs/etl/job.py")           // remote main script
    .pyFile("/tmp/build/hot_fix.zip")                // LOCAL patch → uploaded
    .pyFile("s3a://nx1-libs/python/base_utils.zip")  // remote dep → conf only
    .jar("hdfs://nn/apps/jars/connector-3.1.jar")    // remote jar → conf only
    .name("patched-etl")
    .build();
```

Path classification rules (mirrors the original Python logic):

| Path format | Classification |
|---|---|
| `s3a://`, `hdfs://`, `gs://`, `wasbs://`, `https://` etc. | **Remote** — passed in Spark conf |
| `/absolute/path/that/exists` | **Local** — uploaded |
| `~/relative/path/that/exists` | **Local** — `~` expanded, then uploaded |
| `/path/that/does/not/exist` | **Remote** — assumed remote path without scheme |
| `unknownscheme://anything` | **Remote** — unknown scheme treated as remote |

---

## 6. Passing Arguments and Spark Conf

### Arguments (positional, passed to your script/main class)

```java
// Single args
SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://bucket/job.py")
    .name("my-job")
    .arg("--input-path").arg("s3a://bucket/input/")
    .arg("--output-path").arg("s3a://bucket/output/")
    .arg("--partitions").arg("100")
    .build();

// Or pass a pre-built list
List<String> args = List.of("--date", "2024-03-15", "--mode", "full");
SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://bucket/job.py")
    .name("my-job")
    .args(args)
    .build();
```

### Spark configuration

```java
// Key-by-key
SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://bucket/job.py")
    .name("my-job")
    .conf("spark.executor.memory", "8g")
    .conf("spark.executor.cores", "4")
    .conf("spark.executor.instances", "10")
    .conf("spark.sql.adaptive.enabled", "true")
    .conf("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .build();

// Or from an existing map (e.g. loaded from a config file)
Map<String, String> sparkConf = loadConfFromYaml("spark-defaults.yaml");
SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://bucket/job.py")
    .name("my-job")
    .conf(sparkConf)
    .conf("spark.executor.memory", "8g")  // individual entries override map entries
    .build();
```

> `spark.submit.deployMode=cluster` is always injected automatically — you do not
> need to set it yourself.

---

## 7. Fire-and-Forget with Manual Polling

Use `submit()` to get back a batch ID immediately, then poll or monitor separately.
Useful when you want to submit multiple jobs in parallel and wait on them concurrently.

```java
import com.nx1.kyuubi.model.BatchStatus;

try (KyuubiClient client = new KyuubiClient(config)) {

    // Submit several jobs without waiting
    String batchA = client.submit(SubmitOptions.builder()
        .resource("s3a://bucket/job_a.py").name("job-a").build());

    String batchB = client.submit(SubmitOptions.builder()
        .resource("s3a://bucket/job_b.py").name("job-b").build());

    System.out.println("Submitted: " + batchA + ", " + batchB);

    // Poll status manually
    BatchStatus statusA = client.getStatus(batchA);
    System.out.println("Job A state: " + statusA.getState());
    System.out.println("Job A app ID: " + statusA.getAppId());

    // Block on each independently
    BatchState stateA = client.monitor(batchA);
    BatchState stateB = client.monitor(batchB);

    System.out.println("A=" + stateA + " B=" + stateB);
}
```

### Parallel monitoring with virtual threads (Java 21+)

```java
try (KyuubiClient client = new KyuubiClient(config)) {
    String batchA = client.submit(/* ... */);
    String batchB = client.submit(/* ... */);

    try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
        Future<BatchState> fa = exec.submit(() -> client.monitor(batchA));
        Future<BatchState> fb = exec.submit(() -> client.monitor(batchB));
        System.out.println("A=" + fa.get() + " B=" + fb.get());
    }
}
```

---

## 8. Streaming Logs

Pass a `LogConsumer` (a `@FunctionalInterface`) to `monitor()` or `submitAndWait()`.
Logs are fetched in pages after the job reaches a terminal state.

### Print to stdout

```java
try (KyuubiClient client = new KyuubiClient(config)) {
    client.submitAndWait(opts, System.out::println);
}
```

### Write to SLF4J logger

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

Logger log = LoggerFactory.getLogger(MyClass.class);

try (KyuubiClient client = new KyuubiClient(config)) {
    client.submitAndWait(opts, line -> log.info("[spark] {}", line));
}
```

### Capture into a list

```java
List<String> logLines = new ArrayList<>();

try (KyuubiClient client = new KyuubiClient(config)) {
    BatchState state = client.submitAndWait(opts, logLines::add);
}

// Filter for errors after completion
logLines.stream()
    .filter(line -> line.contains("ERROR") || line.contains("Exception"))
    .forEach(System.err::println);
```

### Stream logs for an already-running batch

```java
try (KyuubiClient client = new KyuubiClient(config)) {
    // You already have a batch ID from a previous run
    client.streamLogs("batch-abc123", System.out::println);
}
```

---

## 9. Cancellation and Signal Handling

### Cancel programmatically

```java
String batchId = client.submit(opts);

// ... later, on timeout or user request:
boolean cancelled = client.cancel(batchId);
System.out.println("Cancelled: " + cancelled);
```

### Cancel on JVM shutdown (shutdown hook)

```java
KyuubiClient client = new KyuubiClient(config);
String batchId = client.submit(opts);

// Register a shutdown hook to cancel the job if the JVM exits
Thread hook = new Thread(() -> {
    try {
        System.err.println("Shutdown signal — cancelling batch " + batchId);
        client.cancel(batchId);
        client.close();
    } catch (Exception e) {
        e.printStackTrace();
    }
});
Runtime.getRuntime().addShutdownHook(hook);

try {
    BatchState state = client.monitor(batchId);
    Runtime.getRuntime().removeShutdownHook(hook);
    // handle state
} finally {
    client.close();
}
```

### Cancel on timeout

```java
try (KyuubiClient client = new KyuubiClient(config)) {
    String batchId = client.submit(opts);

    long deadline = System.currentTimeMillis() + Duration.ofHours(2).toMillis();

    while (true) {
        BatchStatus status = client.getStatus(batchId);
        if (status.getState().isTerminal()) {
            System.out.println("Done: " + status.getState());
            break;
        }
        if (System.currentTimeMillis() > deadline) {
            System.err.println("Timeout — cancelling");
            client.cancel(batchId);
            break;
        }
        Thread.sleep(15_000);
    }
}
```

---

## 10. YuniKorn Queue Routing

Set YuniKorn placement labels directly via Spark configuration.

```java
String queue = "root.default.analytics";

SubmitOptions opts = SubmitOptions.builder()
    .resource("s3a://bucket/etl.py")
    .name("analytics-etl")
    .conf("spark.kubernetes.driver.label.queue",   queue)
    .conf("spark.kubernetes.executor.label.queue", queue)
    .conf("spark.executor.memory", "4g")
    .build();
```

### Helper method (mirrors the Python `inject_yunikorn_spark_configs`)

```java
public static SubmitOptions.Builder withYuniKornQueue(
        SubmitOptions.Builder builder, String queue) {

    // Normalize: ensure it starts with "root."
    String normalized = queue.startsWith("root.")
            ? queue
            : "root.default." + queue;

    return builder
        .conf("spark.kubernetes.driver.label.queue",   normalized)
        .conf("spark.kubernetes.executor.label.queue", normalized);
}

// Usage:
SubmitOptions opts = withYuniKornQueue(
    SubmitOptions.builder()
        .resource("s3a://bucket/job.py")
        .name("my-job"),
    "analytics"           // normalized to root.default.analytics
).build();
```

---

## 11. Using Inside an Airflow PySpark Task

When the library is available on the Spark classpath you can call it from a driver
script. Below is a pattern for an Airflow DAG that submits a child Kyuubi job and
fails the task if the child job fails.

```python
# dags/submit_kyuubi_job.py
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def kyuubi_submission_dag():

    @task.pyspark(conn_id="spark_default")
    def run_etl(logical_date: str, spark, sc):
        """
        This task runs inside Spark. The kyuubi-batch-submitter JAR
        must be available on spark.jars or in the Spark image.
        """
        from py4j.java_gateway import java_import

        gw = spark._jvm
        java_import(gw, "com.nx1.kyuubi.KyuubiClient")
        java_import(gw, "com.nx1.kyuubi.model.KyuubiClientConfig")
        java_import(gw, "com.nx1.kyuubi.model.SubmitOptions")
        java_import(gw, "com.nx1.kyuubi.model.BatchState")

        config = (gw.KyuubiClientConfig.builder()
            .serverUrl(Variable.get("kyuubi_url"))
            .username(Variable.get("kyuubi_user"))
            .password(Variable.get("kyuubi_password"))
            .historyServerUrl(Variable.get("spark_history_url"))
            .build())

        opts = (gw.SubmitOptions.builder()
            .resource("s3a://nx1-jobs/etl/daily_snapshot.py")
            .name(f"daily-snapshot-{logical_date}")
            .conf("spark.executor.memory", "4g")
            .arg("--date").arg(logical_date)
            .build())

        client = gw.KyuubiClient(config)
        try:
            state = client.submitAndWait(opts)
            if str(state) != "FINISHED":
                raise Exception(f"Kyuubi job failed with state: {state}")
        finally:
            client.close()

    run_etl(logical_date="{{ ds }}")

kyuubi_submission_dag()
```

---

## 12. Error Handling Reference

```java
import com.nx1.kyuubi.exception.KyuubiApiException;

try (KyuubiClient client = new KyuubiClient(config)) {

    String batchId = client.submit(opts);
    BatchState state = client.monitor(batchId);

    switch (state) {
        case FINISHED  -> System.out.println("Success");
        case ERROR     -> throw new RuntimeException("Kyuubi job errored");
        case CANCELLED -> System.out.println("Job was cancelled");
        default        -> System.out.println("Unexpected terminal state: " + state);
    }

} catch (KyuubiApiException e) {
    // Non-2xx response from the Kyuubi REST API
    System.err.println("HTTP " + e.getStatusCode() + ": " + e.getResponseBody());

} catch (IOException e) {
    // Transport error (network failure, timeout, etc.)
    System.err.println("Transport error: " + e.getMessage());

} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    System.err.println("Monitoring interrupted");
}
```

### `BatchState` values

| State | `isTerminal()` | Meaning |
|---|---|---|
| `PENDING` | false | Submitted, not yet running |
| `RUNNING` | false | Active on the cluster |
| `FINISHED` | **true** | Reached terminal state (check `appState` for `SUCCEEDED`/`FAILED`) |
| `ERROR` | **true** | Kyuubi-level error (not an app failure) |
| `CANCELLED` | **true** | Cancelled via API |
| `UNKNOWN` | false | Unrecognised string from server |

> **Important:** `FINISHED` does not always mean success. Kyuubi marks the batch
> `FINISHED` when the Spark app completes — but the Spark app itself may have
> failed. Always check `BatchStatus.getAppState()` for `SUCCEEDED`/`FAILED` if
> you need fine-grained exit status. `monitor()` returns the Kyuubi-level
> `BatchState`; use `getStatus()` to inspect `appState` separately.

```java
BatchStatus status = client.getStatus(batchId);
System.out.println("Kyuubi state: " + status.getState());
System.out.println("Spark app state: " + status.getAppState());
System.out.println("Spark app ID: " + status.getAppId());
System.out.println("Diagnostics: " + status.getAppDiagnostic());
```

---

## 13. Full Configuration Reference

### `KyuubiClientConfig.Builder`

| Method | Type | Default | Description |
|---|---|---|---|
| `serverUrl(String)` | required | — | Kyuubi REST base URL |
| `username(String)` | required | — | Basic-auth username |
| `password(String)` | required | — | Basic-auth password |
| `historyServerUrl(String)` | optional | `null` | Spark History Server base URL. When set, log links are formatted as `<url>/history/<appId>/` |
| `pollIntervalMs(long)` | optional | `10000` | How long to wait between status polls (ms) |
| `connectTimeoutMs(int)` | optional | `30000` | HTTP connect timeout (ms) |
| `socketTimeoutMs(int)` | optional | `60000` | HTTP socket/read timeout (ms) |
| `disableSslVerification()` | optional | `false` | Skip TLS cert verification. **Dev only.** |

### `SubmitOptions.Builder`

| Method | Description |
|---|---|
| `resource(String)` | **Required.** Main JAR or Python file. Local paths are auto-uploaded. |
| `name(String)` | **Required.** Display name for the batch job. |
| `className(String)` | Main class for JAR jobs. Ignored for PySpark. |
| `arg(String)` | Append a single positional argument. Chainable. |
| `args(List<String>)` | Replace all positional arguments at once. |
| `conf(String, String)` | Add a single Spark conf key/value. Chainable. |
| `conf(Map<String,String>)` | Merge a map of Spark conf entries. |
| `pyFile(String)` | Python dependency (`.py`, `.zip`, `.egg`). Local or remote. |
| `pyFiles(List<String>)` | Multiple Python dependencies. |
| `jar(String)` | JAR dependency. Local or remote. |
| `jars(List<String>)` | Multiple JAR dependencies. |
| `file(String)` | File to distribute to executors. Local or remote. |
| `files(List<String>)` | Multiple distribution files. |

### `KyuubiClient` methods

| Method | Returns | Description |
|---|---|---|
| `submit(SubmitOptions)` | `String` batchId | Submit job, return immediately |
| `monitor(String batchId)` | `BatchState` | Block until terminal state |
| `monitor(String, LogConsumer)` | `BatchState` | Block + stream logs on completion |
| `submitAndWait(SubmitOptions)` | `BatchState` | submit + monitor in one call |
| `submitAndWait(SubmitOptions, LogConsumer)` | `BatchState` | submit + monitor + stream logs |
| `getStatus(String batchId)` | `BatchStatus` | Non-blocking status poll |
| `cancel(String batchId)` | `boolean` | Cancel a running batch |
| `streamLogs(String, LogConsumer)` | `void` | Fetch and stream all available logs |
| `close()` | `void` | Release HTTP connection pool |
