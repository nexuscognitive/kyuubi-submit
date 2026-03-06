package com.nx1.kyuubi;

import com.nx1.kyuubi.model.KyuubiClientConfig;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.SSLContext;

/**
 * Internal factory that constructs an {@link CloseableHttpClient} from a
 * {@link KyuubiClientConfig}.
 */
final class HttpClientFactory {

    private HttpClientFactory() {}

    static CloseableHttpClient build(KyuubiClientConfig config) {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(config.getUsername(), config.getPassword())
        );

        HttpClientBuilder builder = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .setDefaultRequestConfig(
                        org.apache.http.client.config.RequestConfig.custom()
                                .setConnectTimeout(config.getConnectTimeoutMs())
                                .setSocketTimeout(config.getSocketTimeoutMs())
                                .build()
                );

        if (config.isSslVerificationDisabled()) {
            try {
                SSLContext sslContext = SSLContextBuilder.create()
                        .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                        .build();
                builder.setSSLSocketFactory(
                        new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE)
                );
            } catch (Exception e) {
                throw new IllegalStateException("Failed to configure trust-all SSL context", e);
            }
        }

        return builder.build();
    }
}
