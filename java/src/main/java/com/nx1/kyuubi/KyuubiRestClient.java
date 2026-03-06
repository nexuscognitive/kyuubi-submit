package com.nx1.kyuubi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nx1.kyuubi.exception.KyuubiApiException;
import com.nx1.kyuubi.model.BatchLogResponse;
import com.nx1.kyuubi.model.BatchRequest;
import com.nx1.kyuubi.model.BatchStatus;
import com.nx1.kyuubi.model.KyuubiClientConfig;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Thin wrapper over Apache HttpClient that speaks the Kyuubi REST API.
 *
 * <p>All methods throw {@link KyuubiApiException} on non-2xx responses and
 * {@link IOException} on transport errors.
 */
class KyuubiRestClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KyuubiRestClient.class);

    private final String baseUrl;
    private final CloseableHttpClient http;
    private final ObjectMapper mapper;

    KyuubiRestClient(KyuubiClientConfig config) {
        this.baseUrl = config.getServerUrl().replaceAll("/+$", ""); // strip trailing slashes
        this.http    = HttpClientFactory.build(config);
        this.mapper  = new ObjectMapper();
    }

    // ── Batch lifecycle ────────────────────────────────────────────────────────

    /**
     * Submit a batch using a plain JSON body (no local file uploads).
     *
     * @return the raw JSON response body
     */
    String submitJson(BatchRequest request) throws IOException {
        String url  = baseUrl + "/api/v1/batches";
        String body = mapper.writeValueAsString(request);
        log.debug("POST {} body={}", url, body);

        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));

        return execute(post, 201);
    }

    /**
     * Submit a batch using multipart form data, uploading local files.
     *
     * @param request         the batch request (with {@code resource} set to a placeholder)
     * @param resourceFile    optional main resource file to upload
     * @param localPyFiles    local Python dependency files
     * @param localJars       local JAR dependency files
     * @param localFiles      local distribution files
     * @return the raw JSON response body
     */
    String submitMultipart(
            BatchRequest request,
            File resourceFile,
            List<PathClassifier.LocalFile> localPyFiles,
            List<PathClassifier.LocalFile> localJars,
            List<PathClassifier.LocalFile> localFiles
    ) throws IOException {

        String url = baseUrl + "/api/v1/batches";

        // ── WORKAROUND ──────────────────────────────────────────────────────
        // Kyuubi validates `require(resource != null)` BEFORE processing the
        // uploaded file, then overwrites the field with the temp-file path.
        // We must supply a placeholder so the validation passes.
        if (resourceFile != null) {
            request.resource("placeholder");
        }

        // extraResourcesMap: tells Kyuubi which conf key to bind each extra file to.
        // The map value is a comma-separated list of bare filenames (not full paths).
        buildExtraResourcesMap(request, localPyFiles, localJars, localFiles);

        String requestJson = mapper.writeValueAsString(request);
        log.debug("POST {} (multipart) batchRequest={}", url, requestJson);

        MultipartEntityBuilder mpBuilder = MultipartEntityBuilder.create();

        // Part 1: the JSON batch request
        mpBuilder.addTextBody("batchRequest", requestJson, ContentType.APPLICATION_JSON);

        // Part 2: optional main resource file
        if (resourceFile != null) {
            log.info("Uploading main resource: {}", resourceFile.getAbsolutePath());
            mpBuilder.addBinaryBody(
                    "resourceFile",
                    resourceFile,
                    ContentType.APPLICATION_OCTET_STREAM,
                    resourceFile.getName()
            );
        }

        // Extra files — form field name MUST be the filename (Kyuubi looks them up by name)
        addExtraFiles(mpBuilder, localPyFiles, "pyFile");
        addExtraFiles(mpBuilder, localJars,    "jar");
        addExtraFiles(mpBuilder, localFiles,   "file");

        HttpPost post = new HttpPost(url);
        post.setEntity(mpBuilder.build());

        return execute(post, 200, 201);
    }

    /**
     * Retrieve the current status of a batch.
     */
    BatchStatus getBatchStatus(String batchId) throws IOException {
        String url = baseUrl + "/api/v1/batches/" + batchId;
        HttpGet get = new HttpGet(url);
        String body = executeRaw(get, 200);
        return mapper.readValue(body, BatchStatus.class);
    }

    /**
     * Retrieve a page of local log lines.
     *
     * @param from  zero-based start index
     * @param size  max lines to return
     */
    BatchLogResponse getBatchLogs(String batchId, int from, int size) throws IOException {
        URI uri;
        try {
            uri = new URIBuilder(baseUrl + "/api/v1/batches/" + batchId + "/localLog")
                    .addParameter("from", String.valueOf(from))
                    .addParameter("size", String.valueOf(size))
                    .build();
        } catch (Exception e) {
            throw new IOException("Failed to build log URI", e);
        }

        HttpGet get = new HttpGet(uri);
        String body = executeRaw(get, 200);
        return mapper.readValue(body, BatchLogResponse.class);
    }

    /**
     * Cancel a running batch.
     *
     * @return {@code true} if the cancellation was accepted
     */
    boolean cancelBatch(String batchId) throws IOException {
        String url = baseUrl + "/api/v1/batches/" + batchId;
        HttpDelete delete = new HttpDelete(url);
        try (CloseableHttpResponse resp = http.execute(delete)) {
            int code = resp.getStatusLine().getStatusCode();
            EntityUtils.consumeQuietly(resp.getEntity());
            return code == 200 || code == 204;
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /** Execute and expect one of the given success codes; return the response body. */
    private String execute(org.apache.http.client.methods.HttpUriRequest req, int... successCodes)
            throws IOException {
        return executeRaw(req, successCodes);
    }

    private String executeRaw(org.apache.http.client.methods.HttpUriRequest req, int... successCodes)
            throws IOException {
        try (CloseableHttpResponse resp = http.execute(req)) {
            int    code = resp.getStatusLine().getStatusCode();
            HttpEntity entity = resp.getEntity();
            String body = entity != null ? EntityUtils.toString(entity, "UTF-8") : "";

            for (int sc : successCodes) {
                if (code == sc) return body;
            }
            throw new KyuubiApiException(code, body);
        }
    }

    private void addExtraFiles(
            MultipartEntityBuilder builder,
            List<PathClassifier.LocalFile> files,
            String logLabel
    ) {
        for (PathClassifier.LocalFile lf : files) {
            File f = new File(lf.resolved());
            log.info("Uploading {} file: {}", logLabel, f.getAbsolutePath());
            // Field name MUST be the bare filename — Kyuubi resolves by name.
            builder.addBinaryBody(
                    f.getName(),
                    f,
                    ContentType.APPLICATION_OCTET_STREAM,
                    f.getName()
            );
        }
    }

    private void buildExtraResourcesMap(
            BatchRequest request,
            List<PathClassifier.LocalFile> localPyFiles,
            List<PathClassifier.LocalFile> localJars,
            List<PathClassifier.LocalFile> localFiles
    ) {
        if (localPyFiles.isEmpty() && localJars.isEmpty() && localFiles.isEmpty()) return;

        Map<String, String> extraMap = new java.util.LinkedHashMap<>();

        if (!localPyFiles.isEmpty()) {
            extraMap.put("spark.submit.pyFiles",
                    joinNames(localPyFiles));
        }
        if (!localJars.isEmpty()) {
            extraMap.put("spark.jars",
                    joinNames(localJars));
        }
        if (!localFiles.isEmpty()) {
            extraMap.put("spark.files",
                    joinNames(localFiles));
        }

        request.extraResourcesMap(extraMap);
        log.info("Extra resources map: {}", extraMap);
    }

    private String joinNames(List<PathClassifier.LocalFile> files) {
        return files.stream()
                .map(lf -> new File(lf.resolved()).getName())
                .collect(java.util.stream.Collectors.joining(","));
    }

    @Override
    public void close() throws IOException {
        http.close();
    }
}
