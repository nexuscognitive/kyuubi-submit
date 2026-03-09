package com.nx1.kyuubi;

import com.nx1.kyuubi.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URL;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that submits a real PySpark job to a live Kyuubi instance.
 * Submits the job via {@code submit()}, then blocks until it reaches a terminal state
 * via {@code monitor()}, and asserts the final state is {@code FINISHED}.
 *
 * <p>Skipped unless all three environment variables are set:
 * <ul>
 *   <li>{@code KYUUBI_SERVER_URL}</li>
 *   <li>{@code KYUUBI_USERNAME}</li>
 *   <li>{@code KYUUBI_PASSWORD}</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * KYUUBI_SERVER_URL=https://kyuubi.example.com \
 * KYUUBI_USERNAME=user \
 * KYUUBI_PASSWORD=pass \
 * mvn verify -Dit.test=KyuubiClientIT
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "KYUUBI_SERVER_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "KYUUBI_USERNAME", matches = ".+")
@EnabledIfEnvironmentVariable(named = "KYUUBI_PASSWORD", matches = ".+")
class KyuubiClientIT {

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void submitAndWaitPySparkJob() throws Exception {
        String serverUrl = System.getenv("KYUUBI_SERVER_URL");
        String username  = System.getenv("KYUUBI_USERNAME");
        String password  = System.getenv("KYUUBI_PASSWORD");

        URL jobUrl = getClass().getClassLoader().getResource("test-job.py");
        assertNotNull(jobUrl, "test-job.py not found in test resources");
        String resource = Paths.get(jobUrl.toURI()).toString();

        KyuubiClientConfig config = KyuubiClientConfig.builder()
                .serverUrl(serverUrl)
                .username(username)
                .password(password)
                .disableSslVerification()
                .pollIntervalMs(5_000)
                .build();

        SubmitOptions opts = SubmitOptions.builder()
                .resource(resource)
                .name("java-client-integration-test")
                .build();

        String batchId = null;
        KyuubiClient client = new KyuubiClient(config);
        try {
            batchId = client.submit(opts);
            assertNotNull(batchId, "Batch submission should return a non-null batch ID");

            BatchState state = client.monitor(batchId, System.out::println);

            if (state != BatchState.FINISHED) {
                BatchStatus status = client.getStatus(batchId);
                fail("Batch " + batchId + " ended in state " + state
                        + " (appState=" + status.getAppState()
                        + ", diagnostic=" + status.getAppDiagnostic() + ")");
            }
        } catch (Exception | AssertionError e) {
            if (batchId != null) {
                try {
                    client.cancel(batchId);
                } catch (Exception cancelEx) {
                    e.addSuppressed(cancelEx);
                }
            }
            throw e;
        } finally {
            client.close();
        }
    }
}
