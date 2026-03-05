package com.nx1.kyuubi;

import com.nx1.kyuubi.exception.KyuubiApiException;
import com.nx1.kyuubi.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * High-level client for submitting and monitoring Kyuubi batch jobs.
 *
 * <p>Typical usage:
 * <pre>{@code
 * KyuubiClientConfig cfg = KyuubiClientConfig.builder()
 *     .serverUrl("https://kyuubi.internal:10099")
 *     .username("svc-spark")
 *     .password(System.getenv("KYUUBI_PASSWORD"))
 *     .historyServerUrl("http://spark-history:18080")
 *     .disableSslVerification()   // dev only
 *     .build();
 *
 * SubmitOptions opts = SubmitOptions.builder()
 *     .resource("s3a://my-bucket/jobs/etl.py")
 *     .name("daily-etl")
 *     .conf("spark.executor.memory", "4g")
 *     .conf("spark.executor.cores", "2")
 *     .arg("--date").arg("2024-01-15")
 *     .build();
 *
 * try (KyuubiClient client = new KyuubiClient(cfg)) {
 *     String batchId = client.submit(opts);
 *     String finalState = client.monitor(batchId);
 * }
 * }</pre>
 *
 * <p>The client is thread-safe; a single instance may submit multiple jobs
 * concurrently.
 */
public class KyuubiClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KyuubiClient.class);

    /** Python / zip / egg file extensions that imply a PYSPARK batch type. */
    private static final Set<String> PYTHON_EXTENSIONS = Set.of(".py", ".zip", ".egg");

    private final KyuubiClientConfig config;
    private final KyuubiRestClient   rest;
    private final PathClassifier     classifier;

    // ── Constructor ────────────────────────────────────────────────────────────

    public KyuubiClient(KyuubiClientConfig config) {
        this.config     = config;
        this.rest       = new KyuubiRestClient(config);
        this.classifier = PathClassifier.INSTANCE;
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    /**
     * Submit a batch job and return its ID immediately (fire-and-forget).
     *
     * @param options submission options
     * @return Kyuubi batch ID
     * @throws IOException        on transport errors
     * @throws KyuubiApiException on non-2xx API responses
     */
    public String submit(SubmitOptions options) throws IOException {
        String resource   = options.getResource();
        boolean isPySpark = isPythonResource(resource);
        BatchType type    = isPySpark ? BatchType.PYSPARK : BatchType.SPARK;
        log.info("Detected batch type: {}", type);

        boolean resourceIsLocal = classifier.isLocal(resource);
        String  resourceResolved = resourceIsLocal ? classifier.resolve(resource) : null;

        // Classify extra files
        PathClassifier.Classification pyClassified  = classifier.classify(options.getPyFiles());
        PathClassifier.Classification jarClassified = classifier.classify(options.getJars());
        PathClassifier.Classification fileClassified= classifier.classify(options.getFiles());

        // Build the Spark conf map
        Map<String, String> conf = buildConf(options, pyClassified, jarClassified, fileClassified);

        // Build the batch request object
        BatchRequest request = new BatchRequest(type, options.getName())
                .conf(conf)
                .args(options.getArgs());

        if (!resourceIsLocal) {
            request.resource(resource);
        }
        if (!isPySpark && options.getClassName() != null && !options.getClassName().isBlank()) {
            request.className(options.getClassName());
        }

        boolean hasLocalFiles = resourceIsLocal
                || !pyClassified.localFiles.isEmpty()
                || !jarClassified.localFiles.isEmpty()
                || !fileClassified.localFiles.isEmpty();

        String batchId;
        if (hasLocalFiles) {
            log.info("Detected local files — using multipart upload");
            File resourceFile = resourceIsLocal ? new File(resourceResolved) : null;
            batchId = rest.submitMultipart(
                    request,
                    resourceFile,
                    pyClassified.localFiles,
                    jarClassified.localFiles,
                    fileClassified.localFiles
            );
        } else {
            log.info("All resources are remote — using JSON submission");
            batchId = parseBatchId(rest.submitJson(request));
        }

        log.info("Batch submitted successfully. ID: {}", batchId);
        return batchId;
    }

    /**
     * Block until the batch reaches a terminal state and return the final state.
     *
     * <p>Status is polled every {@link KyuubiClientConfig#getPollIntervalMs()} ms.
     *
     * @param batchId the ID returned by {@link #submit(SubmitOptions)}
     * @return the final {@link BatchState}
     */
    public BatchState monitor(String batchId) throws IOException, InterruptedException {
        return monitor(batchId, /* logConsumer */ null);
    }

    /**
     * Block until the batch reaches a terminal state.
     *
     * @param batchId     the batch to monitor
     * @param logConsumer optional callback invoked with each log line once the
     *                    job finishes; pass {@code null} to skip log retrieval
     * @return the final {@link BatchState}
     */
    public BatchState monitor(String batchId, LogConsumer logConsumer)
            throws IOException, InterruptedException {

        log.info("Monitoring batch {}", batchId);

        long    startMs      = System.currentTimeMillis();
        String  prevState    = null;
        String  prevAppState = null;

        while (true) {
            BatchStatus status = rest.getBatchStatus(batchId);
            BatchState  state  = status.getState();
            String      appState = status.getAppState();
            String      appId    = status.getAppId();

            boolean stateChanged = !Objects.equals(status.getRawState(), prevState)
                    || !Objects.equals(appState, prevAppState);

            if (stateChanged) {
                long elapsedSec = (System.currentTimeMillis() - startMs) / 1000;
                log.info("Batch state: {} | App state: {} | elapsed: {}s",
                        status.getRawState(),
                        appState != null ? appState : "N/A",
                        elapsedSec);

                if (appId != null && !appId.isBlank()) {
                    log.info("Spark App ID: {}", appId);
                    String historyUrl = formatHistoryUrl(appId, status.getAppUrl());
                    if (historyUrl != null) {
                        log.info("Spark History URL: {}", historyUrl);
                    }
                }

                prevState    = status.getRawState();
                prevAppState = appState;
            }

            if (state.isTerminal()) {
                long elapsedMs  = System.currentTimeMillis() - startMs;
                long elapsedMin = elapsedMs / 60_000;
                long elapsedSec = (elapsedMs % 60_000) / 1000;

                log.info("Job finished in {}m {}s — final state: {} / appState: {}",
                        elapsedMin, elapsedSec,
                        state,
                        appState != null ? appState : "N/A");

                String diagnostic = status.getAppDiagnostic();
                if (diagnostic != null && !diagnostic.isBlank()) {
                    log.info("Application diagnostics: {}", diagnostic);
                }

                if (logConsumer != null) {
                    fetchAndStreamLogs(batchId, logConsumer);
                }

                return state;
            }

            Thread.sleep(config.getPollIntervalMs());
        }
    }

    /**
     * Cancel a running batch.
     *
     * @param batchId the batch to cancel
     * @return {@code true} if the server accepted the cancellation
     */
    public boolean cancel(String batchId) throws IOException {
        log.info("Cancelling batch {}…", batchId);
        boolean ok = rest.cancelBatch(batchId);
        if (ok) {
            log.info("Batch {} cancelled successfully", batchId);
        } else {
            log.warn("Batch {} cancellation request was not acknowledged", batchId);
        }
        return ok;
    }

    /**
     * Retrieve the current status of a batch without blocking.
     */
    public BatchStatus getStatus(String batchId) throws IOException {
        return rest.getBatchStatus(batchId);
    }

    /**
     * Stream all available log lines to the given consumer.
     */
    public void streamLogs(String batchId, LogConsumer consumer) throws IOException {
        fetchAndStreamLogs(batchId, consumer);
    }

    // ── Convenience methods ────────────────────────────────────────────────────

    /**
     * Submit a job and block until completion, returning the final {@link BatchState}.
     * This is the single-call "fire-and-wait" convenience method.
     */
    public BatchState submitAndWait(SubmitOptions options) throws IOException, InterruptedException {
        return submitAndWait(options, null);
    }

    /**
     * Submit a job, wait for completion, and stream logs to {@code logConsumer}.
     */
    public BatchState submitAndWait(SubmitOptions options, LogConsumer logConsumer)
            throws IOException, InterruptedException {
        String batchId = submit(options);
        return monitor(batchId, logConsumer);
    }

    // ── LogConsumer functional interface ──────────────────────────────────────

    /**
     * Callback for receiving log lines produced by a batch job.
     */
    @FunctionalInterface
    public interface LogConsumer {
        void accept(String line);
    }

    // ── Closeable ─────────────────────────────────────────────────────────────

    @Override
    public void close() throws IOException {
        rest.close();
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private boolean isPythonResource(String resource) {
        String lower = resource.toLowerCase();
        // Strip any URI scheme / query before checking extension
        int lastSlash = Math.max(lower.lastIndexOf('/'), lower.lastIndexOf('\\'));
        String filename = lower.substring(lastSlash + 1);
        // Strip query params
        int qMark = filename.indexOf('?');
        if (qMark >= 0) filename = filename.substring(0, qMark);
        for (String ext : PYTHON_EXTENSIONS) {
            if (filename.endsWith(ext)) return true;
        }
        return false;
    }

    private Map<String, String> buildConf(
            SubmitOptions options,
            PathClassifier.Classification pyClassified,
            PathClassifier.Classification jarClassified,
            PathClassifier.Classification fileClassified
    ) {
        Map<String, String> conf = new LinkedHashMap<>(options.getConf());

        // Always deploy in cluster mode
        conf.put("spark.submit.deployMode", "cluster");

        // Remote py-files appended to conf
        if (!pyClassified.remoteUris.isEmpty()) {
            String existing = conf.getOrDefault("spark.submit.pyFiles", "");
            String merged   = merge(existing, String.join(",", pyClassified.remoteUris));
            conf.put("spark.submit.pyFiles", merged);
        }

        // Remote jars appended to conf
        if (!jarClassified.remoteUris.isEmpty()) {
            String existing = conf.getOrDefault("spark.jars", "");
            String merged   = merge(existing, String.join(",", jarClassified.remoteUris));
            conf.put("spark.jars", merged);
        }

        // Remote distribution files appended to conf
        if (!fileClassified.remoteUris.isEmpty()) {
            String existing = conf.getOrDefault("spark.files", "");
            String merged   = merge(existing, String.join(",", fileClassified.remoteUris));
            conf.put("spark.files", merged);
        }

        return conf;
    }

    /** Merge two comma-separated lists, ignoring blanks. */
    private static String merge(String a, String b) {
        if (a == null || a.isBlank()) return b;
        if (b == null || b.isBlank()) return a;
        return a + "," + b;
    }

    private String parseBatchId(String responseBody) throws IOException {
        // submitJson returns the raw response body; we need to extract "id"
        try {
            Map<?, ?> map = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(responseBody, Map.class);
            Object id = map.get("id");
            if (id == null) throw new IOException("No 'id' field in batch response: " + responseBody);
            return id.toString();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IOException("Could not parse batch response as JSON: " + responseBody, e);
        }
    }

    private void fetchAndStreamLogs(String batchId, LogConsumer consumer) throws IOException {
        log.info("Retrieving job logs for batch {}", batchId);
        int from    = 0;
        int size    = 1000;
        int printed = 0;

        while (true) {
            BatchLogResponse logResp = rest.getBatchLogs(batchId, from, size);
            List<String> rows = logResp.getLogRowSet();
            for (String line : rows) {
                consumer.accept(line);
                printed++;
            }
            if (from + rows.size() >= logResp.getTotal()) break;
            from += rows.size();
        }
        log.info("Retrieved {} log lines", printed);
    }

    private String formatHistoryUrl(String appId, String appUrl) {
        String historyBase = config.getHistoryServerUrl();
        if (historyBase != null && !historyBase.isBlank() && appId != null && !appId.isBlank()) {
            historyBase = historyBase.replaceAll("/+$", "");
            if (!historyBase.startsWith("http://") && !historyBase.startsWith("https://")) {
                historyBase = "http://" + historyBase;
            }
            // Append default port if none specified
            if (!historyBase.matches(".*:\\d+$")) {
                historyBase = historyBase + ":18080";
            }
            return historyBase + "/history/" + appId + "/";
        }
        return appUrl;
    }
}
