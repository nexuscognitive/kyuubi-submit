package com.nx1.kyuubi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Classifies file path strings as either <em>local</em> (needs uploading) or
 * <em>remote</em> (passed as-is in the Spark config).
 *
 * <p>Logic mirrors the Python {@code KyuubiBatchSubmitter.is_local_file()} method:
 * <ol>
 *   <li>If the URI scheme is a known remote scheme → remote.</li>
 *   <li>If the URI has an unknown scheme (length &gt; 1 to skip Windows drive
 *       letters) → remote.</li>
 *   <li>If the path exists on the local filesystem → local.</li>
 *   <li>Otherwise → remote (assume a relative remote path without a scheme).</li>
 * </ol>
 */
class PathClassifier {

    private static final Logger log = LoggerFactory.getLogger(PathClassifier.class);

    private static final Set<String> REMOTE_SCHEMES = Set.of(
            "hdfs", "s3", "s3a", "s3n", "gs",
            "wasb", "wasbs", "abfs", "abfss",
            "http", "https", "ftp", "local"
    );

    // Singleton — no state
    static final PathClassifier INSTANCE = new PathClassifier();

    private PathClassifier() {}

    // ── Public API ─────────────────────────────────────────────────────────────

    /** Holds the result of classifying a collection of path strings. */
    static final class Classification {
        /** Pairs of (original path, resolved absolute path) for local files. */
        final List<LocalFile> localFiles = new ArrayList<>();
        /** Remote URIs / paths that need no uploading. */
        final List<String>    remoteUris = new ArrayList<>();
    }

    record LocalFile(String original, String resolved) {}

    /**
     * Classify each path in {@code paths} into local or remote.
     */
    Classification classify(List<String> paths) {
        Classification result = new Classification();
        if (paths == null) return result;

        for (String raw : paths) {
            if (raw == null || raw.isBlank()) continue;
            String path = raw.strip();
            if (isLocal(path)) {
                result.localFiles.add(new LocalFile(path, resolve(path)));
            } else {
                result.remoteUris.add(path);
            }
        }
        return result;
    }

    boolean isLocal(String path) {
        if (path == null || path.isBlank()) return false;

        // Try to parse as a URI to extract the scheme
        String scheme = extractScheme(path);

        if (scheme != null) {
            if (REMOTE_SCHEMES.contains(scheme.toLowerCase())) {
                return false; // known remote
            }
            // Unknown scheme with length > 1 (to skip Windows "C:")
            if (scheme.length() > 1) {
                log.debug("Unknown URI scheme '{}' for '{}', treating as remote", scheme, path);
                return false;
            }
        }

        // No scheme (or single-char scheme = Windows drive letter) → check disk
        File file = new File(resolve(path));
        if (file.isFile()) {
            return true;
        }

        log.debug("Path '{}' not found locally, treating as remote", path);
        return false;
    }

    String resolve(String path) {
        // Expand ~ to home directory (Java has no built-in for this)
        if (path.startsWith("~/")) {
            return System.getProperty("user.home") + path.substring(1);
        }
        return Paths.get(path).toAbsolutePath().toString();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private String extractScheme(String path) {
        try {
            URI uri = new URI(path);
            return uri.getScheme(); // null if no scheme
        } catch (URISyntaxException e) {
            return null; // invalid URI — treat as local path
        }
    }
}
