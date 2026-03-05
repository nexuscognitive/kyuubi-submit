package com.nx1.kyuubi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PathClassifierTest {

    private final PathClassifier clf = PathClassifier.INSTANCE;

    // ── isLocal ───────────────────────────────────────────────────────────────

    @Test
    void knownRemoteSchemesAreNotLocal() {
        assertFalse(clf.isLocal("s3a://my-bucket/path/file.py"));
        assertFalse(clf.isLocal("hdfs://namenode/user/spark/job.jar"));
        assertFalse(clf.isLocal("gs://bucket/file.jar"));
        assertFalse(clf.isLocal("https://repo.example.com/dep.jar"));
    }

    @Test
    void unknownSchemeTreatedAsRemote() {
        assertFalse(clf.isLocal("fooscheme://something/file.py"));
    }

    @Test
    void existingLocalFileIsLocal(@TempDir Path tmp) throws IOException {
        File f = tmp.resolve("job.py").toFile();
        assertTrue(f.createNewFile());
        assertTrue(clf.isLocal(f.getAbsolutePath()));
    }

    @Test
    void nonExistentPathTreatedAsRemote() {
        assertFalse(clf.isLocal("/nonexistent/path/to/job.jar"));
    }

    @Test
    void nullAndBlankReturnFalse() {
        assertFalse(clf.isLocal(null));
        assertFalse(clf.isLocal("   "));
    }

    // ── classify ──────────────────────────────────────────────────────────────

    @Test
    void classifyMixedPaths(@TempDir Path tmp) throws IOException {
        File local1 = tmp.resolve("util.py").toFile();
        File local2 = tmp.resolve("dep.zip").toFile();
        assertTrue(local1.createNewFile());
        assertTrue(local2.createNewFile());

        List<String> paths = List.of(
                local1.getAbsolutePath(),
                "s3a://bucket/remote.py",
                local2.getAbsolutePath(),
                "hdfs://nn/remote2.zip"
        );

        PathClassifier.Classification result = clf.classify(paths);

        assertEquals(2, result.localFiles.size());
        assertEquals(2, result.remoteUris.size());
        assertTrue(result.remoteUris.contains("s3a://bucket/remote.py"));
        assertTrue(result.remoteUris.contains("hdfs://nn/remote2.zip"));
    }

    @Test
    void classifyNullListReturnsEmpty() {
        PathClassifier.Classification result = clf.classify(null);
        assertTrue(result.localFiles.isEmpty());
        assertTrue(result.remoteUris.isEmpty());
    }
}
