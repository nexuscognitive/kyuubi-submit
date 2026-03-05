package com.nx1.kyuubi.model;

import java.util.Objects;

/**
 * Connection and behaviour settings for {@link com.nx1.kyuubi.KyuubiClient}.
 * <p>
 * Build instances with {@link Builder}:
 * <pre>{@code
 * KyuubiClientConfig cfg = KyuubiClientConfig.builder()
 *     .serverUrl("https://kyuubi.internal:10099")
 *     .username("spark")
 *     .password("secret")
 *     .historyServerUrl("http://spark-history:18080")
 *     .build();
 * }</pre>
 */
public final class KyuubiClientConfig {

    private final String serverUrl;
    private final String username;
    private final String password;
    private final String historyServerUrl;

    /** Polling interval in milliseconds while waiting for job completion. */
    private final long pollIntervalMs;

    /** Connection timeout in milliseconds for each HTTP request. */
    private final int connectTimeoutMs;

    /** Socket / read timeout in milliseconds for each HTTP request. */
    private final int socketTimeoutMs;

    /** Whether to skip TLS certificate verification (useful in dev/test). */
    private final boolean sslVerificationDisabled;

    private KyuubiClientConfig(Builder b) {
        this.serverUrl               = Objects.requireNonNull(b.serverUrl,  "serverUrl must not be null");
        this.username                = Objects.requireNonNull(b.username,   "username must not be null");
        this.password                = Objects.requireNonNull(b.password,   "password must not be null");
        this.historyServerUrl        = b.historyServerUrl;
        this.pollIntervalMs          = b.pollIntervalMs;
        this.connectTimeoutMs        = b.connectTimeoutMs;
        this.socketTimeoutMs         = b.socketTimeoutMs;
        this.sslVerificationDisabled = b.sslVerificationDisabled;
    }

    public String getServerUrl()            { return serverUrl; }
    public String getUsername()             { return username; }
    public String getPassword()             { return password; }
    public String getHistoryServerUrl()     { return historyServerUrl; }
    public long   getPollIntervalMs()       { return pollIntervalMs; }
    public int    getConnectTimeoutMs()     { return connectTimeoutMs; }
    public int    getSocketTimeoutMs()      { return socketTimeoutMs; }
    public boolean isSslVerificationDisabled() { return sslVerificationDisabled; }

    // ── Builder ────────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {

        private String  serverUrl;
        private String  username;
        private String  password;
        private String  historyServerUrl        = null;
        private long    pollIntervalMs          = 10_000L;
        private int     connectTimeoutMs        = 30_000;
        private int     socketTimeoutMs         = 60_000;
        private boolean sslVerificationDisabled = false;

        /** Base URL of the Kyuubi REST server, e.g. {@code https://kyuubi:10099}. */
        public Builder serverUrl(String url) {
            this.serverUrl = url;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Optional Spark History Server base URL.
         * When set, log URLs are formatted as
         * {@code <historyServerUrl>/history/<appId>/}.
         */
        public Builder historyServerUrl(String url) {
            this.historyServerUrl = url;
            return this;
        }

        /** How long to wait between status polls (default: 10 000 ms). */
        public Builder pollIntervalMs(long ms) {
            this.pollIntervalMs = ms;
            return this;
        }

        /** HTTP connect timeout (default: 30 000 ms). */
        public Builder connectTimeoutMs(int ms) {
            this.connectTimeoutMs = ms;
            return this;
        }

        /** HTTP socket/read timeout (default: 60 000 ms). */
        public Builder socketTimeoutMs(int ms) {
            this.socketTimeoutMs = ms;
            return this;
        }

        /**
         * Disable TLS certificate validation.
         * <strong>Do not use in production.</strong>
         */
        public Builder disableSslVerification() {
            this.sslVerificationDisabled = true;
            return this;
        }

        public KyuubiClientConfig build() { return new KyuubiClientConfig(this); }
    }
}
