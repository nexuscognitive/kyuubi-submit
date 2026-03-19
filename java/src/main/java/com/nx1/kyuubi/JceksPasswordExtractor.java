package com.nx1.kyuubi;

import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;

/**
 * Extracts a password from a JCEKS keystore file.
 * <p>
 * If the password supplied to the Kyuubi client is a path to a {@code .jceks} file,
 * this class loads the keystore (with a {@code null} store password) and retrieves
 * the secret key whose alias matches the username.
 */
final class JceksPasswordExtractor {

    private JceksPasswordExtractor() {}

    /**
     * Returns {@code true} if {@code password} looks like a path to a JCEKS keystore file.
     */
    static boolean isJceksPath(String password) {
        return password != null
                && password.endsWith(".jceks")
                && Files.isRegularFile(Path.of(password));
    }

    /**
     * Extracts the password stored under the given alias in a JCEKS keystore.
     *
     * @param jceksPath path to the {@code .jceks} file
     * @param alias     the alias to look up (typically the username)
     * @return the password string stored in the keystore
     * @throws IllegalArgumentException if the alias is not found or the entry is not a secret key
     * @throws IllegalStateException    if the keystore cannot be loaded
     */
    static String extract(String jceksPath, String alias) {
        try (FileInputStream fis = new FileInputStream(jceksPath)) {
            KeyStore ks = KeyStore.getInstance("JCEKS");
            ks.load(fis, "none".toCharArray());

            if (!ks.containsAlias(alias)) {
                throw new IllegalArgumentException(
                        "Alias '" + alias + "' not found in JCEKS keystore '" + jceksPath + "'");
            }

            KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) ks.getEntry(
                    alias, new KeyStore.PasswordProtection("none".toCharArray()));
            if (entry == null) {
                throw new IllegalArgumentException(
                        "Alias '" + alias + "' in '" + jceksPath + "' is not a secret key entry");
            }

            SecretKey key = entry.getSecretKey();
            return new String(key.getEncoded(), java.nio.charset.StandardCharsets.UTF_8);

        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
            throw new IllegalStateException("Failed to load JCEKS keystore '" + jceksPath + "'", e);
        } catch (UnrecoverableEntryException e) {
            throw new IllegalStateException("Failed to recover entry '" + alias + "' from '" + jceksPath + "'", e);
        }
    }
}
