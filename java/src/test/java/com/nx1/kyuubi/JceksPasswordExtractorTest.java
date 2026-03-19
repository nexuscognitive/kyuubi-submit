package com.nx1.kyuubi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;

import static org.junit.jupiter.api.Assertions.*;

class JceksPasswordExtractorTest {

    /**
     * Helper that creates a JCEKS keystore file containing a single secret-key entry.
     */
    private static Path createJceksKeystore(Path dir, String alias, String secret) throws Exception {
        KeyStore ks = KeyStore.getInstance("JCEKS");
        ks.load(null, "none".toCharArray()); // initialise empty keystore

        SecretKey key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "AES");
        ks.setEntry(alias, new KeyStore.SecretKeyEntry(key),
                new KeyStore.PasswordProtection("none".toCharArray()));

        Path file = dir.resolve("test.jceks");
        try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
            ks.store(fos, "none".toCharArray());
        }
        return file;
    }

    @Test
    void extractReturnsStoredPassword(@TempDir Path tmp) throws Exception {
        Path jceks = createJceksKeystore(tmp, "myuser", "s3cret!");
        String result = JceksPasswordExtractor.extract(jceks.toString(), "myuser");
        assertEquals("s3cret!", result);
    }

    @Test
    void extractThrowsOnMissingAlias(@TempDir Path tmp) throws Exception {
        Path jceks = createJceksKeystore(tmp, "myuser", "s3cret!");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> JceksPasswordExtractor.extract(jceks.toString(), "wrong-alias"));
        assertTrue(ex.getMessage().contains("wrong-alias"));
        assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    void extractThrowsOnMissingFile() {
        assertThrows(IllegalStateException.class,
                () -> JceksPasswordExtractor.extract("/nonexistent/path.jceks", "alias"));
    }

    @Test
    void isJceksPathReturnsTrueForExistingJceksFile(@TempDir Path tmp) throws Exception {
        Path jceks = createJceksKeystore(tmp, "a", "b");
        assertTrue(JceksPasswordExtractor.isJceksPath(jceks.toString()));
    }

    @Test
    void isJceksPathReturnsFalseForPlainPassword() {
        assertFalse(JceksPasswordExtractor.isJceksPath("my-plain-password"));
    }

    @Test
    void isJceksPathReturnsFalseForNonExistentJceksPath() {
        assertFalse(JceksPasswordExtractor.isJceksPath("/no/such/file.jceks"));
    }

    @Test
    void isJceksPathReturnsFalseForNull() {
        assertFalse(JceksPasswordExtractor.isJceksPath(null));
    }
}
