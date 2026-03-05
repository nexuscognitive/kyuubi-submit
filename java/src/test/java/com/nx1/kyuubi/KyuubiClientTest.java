package com.nx1.kyuubi;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.nx1.kyuubi.model.BatchState;
import com.nx1.kyuubi.model.KyuubiClientConfig;
import com.nx1.kyuubi.model.SubmitOptions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

class KyuubiClientTest {

    private static WireMockServer wm;

    @BeforeAll
    static void startWireMock() {
        wm = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wm.start();
    }

    @AfterAll
    static void stopWireMock() {
        wm.stop();
    }

    @BeforeEach
    void resetStubs() {
        wm.resetAll();
    }

    private KyuubiClient newClient() {
        KyuubiClientConfig cfg = KyuubiClientConfig.builder()
                .serverUrl("http://localhost:" + wm.port())
                .username("test")
                .password("test")
                .pollIntervalMs(50)
                .build();
        return new KyuubiClient(cfg);
    }

    // ── submit (JSON path) ─────────────────────────────────────────────────────

    @Test
    void submitRemoteResourceUsesJsonPath() throws Exception {
        wm.stubFor(post(urlEqualTo("/api/v1/batches"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"batch-001\",\"batchType\":\"PYSPARK\",\"state\":\"PENDING\"}")));

        SubmitOptions opts = SubmitOptions.builder()
                .resource("s3a://bucket/job.py")
                .name("test-job")
                .build();

        try (KyuubiClient client = newClient()) {
            String batchId = client.submit(opts);
            assertEquals("batch-001", batchId);
        }

        wm.verify(postRequestedFor(urlEqualTo("/api/v1/batches"))
                .withHeader("Content-Type", containing("application/json")));
    }

    // ── submit (multipart path) ────────────────────────────────────────────────

    @Test
    void submitLocalResourceUsesMultipart(@TempDir Path tmp) throws Exception {
        // Create a local Python file
        File script = tmp.resolve("job.py").toFile();
        Files.writeString(script.toPath(), "print('hello')");

        wm.stubFor(post(urlEqualTo("/api/v1/batches"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"batch-002\",\"batchType\":\"PYSPARK\",\"state\":\"PENDING\"}")));

        SubmitOptions opts = SubmitOptions.builder()
                .resource(script.getAbsolutePath())
                .name("local-job")
                .build();

        try (KyuubiClient client = newClient()) {
            String batchId = client.submit(opts);
            assertEquals("batch-002", batchId);
        }

        // Verify multipart content-type was used
        wm.verify(postRequestedFor(urlEqualTo("/api/v1/batches"))
                .withHeader("Content-Type", containing("multipart/form-data")));
    }

    // ── monitor ────────────────────────────────────────────────────────────────

    @Test
    void monitorPollsUntilFinished() throws Exception {
        // Sequence: PENDING → RUNNING → FINISHED
        wm.stubFor(get(urlEqualTo("/api/v1/batches/batch-003"))
                .inScenario("lifecycle")
                .whenScenarioStateIs("Started")
                .willReturn(okJson("{\"id\":\"batch-003\",\"state\":\"PENDING\"}"))
                .willSetStateTo("running"));

        wm.stubFor(get(urlEqualTo("/api/v1/batches/batch-003"))
                .inScenario("lifecycle")
                .whenScenarioStateIs("running")
                .willReturn(okJson("{\"id\":\"batch-003\",\"state\":\"RUNNING\",\"appId\":\"app-01\",\"appState\":\"RUNNING\"}"))
                .willSetStateTo("done"));

        wm.stubFor(get(urlEqualTo("/api/v1/batches/batch-003"))
                .inScenario("lifecycle")
                .whenScenarioStateIs("done")
                .willReturn(okJson("{\"id\":\"batch-003\",\"state\":\"FINISHED\",\"appId\":\"app-01\",\"appState\":\"SUCCEEDED\"}")));

        try (KyuubiClient client = newClient()) {
            BatchState state = client.monitor("batch-003");
            assertEquals(BatchState.FINISHED, state);
        }
    }

    @Test
    void monitorStreamsLogsOnCompletion() throws Exception {
        wm.stubFor(get(urlEqualTo("/api/v1/batches/batch-004"))
                .willReturn(okJson("{\"id\":\"batch-004\",\"state\":\"FINISHED\",\"appState\":\"SUCCEEDED\"}")));

        wm.stubFor(get(urlPathEqualTo("/api/v1/batches/batch-004/localLog"))
                .willReturn(okJson("{\"logRowSet\":[\"line1\",\"line2\"],\"total\":2}")));

        List<String> captured = new ArrayList<>();

        try (KyuubiClient client = newClient()) {
            client.monitor("batch-004", captured::add);
        }

        assertEquals(List.of("line1", "line2"), captured);
    }

    // ── cancel ─────────────────────────────────────────────────────────────────

    @Test
    void cancelSendsDeleteRequest() throws Exception {
        wm.stubFor(delete(urlEqualTo("/api/v1/batches/batch-005"))
                .willReturn(aResponse().withStatus(200)));

        try (KyuubiClient client = newClient()) {
            assertTrue(client.cancel("batch-005"));
        }
    }

    // ── BatchState helpers ─────────────────────────────────────────────────────

    @Test
    void batchStateTerminalCheck() {
        assertTrue(BatchState.FINISHED.isTerminal());
        assertTrue(BatchState.ERROR.isTerminal());
        assertTrue(BatchState.CANCELLED.isTerminal());
        assertFalse(BatchState.RUNNING.isTerminal());
        assertFalse(BatchState.PENDING.isTerminal());
    }
}
