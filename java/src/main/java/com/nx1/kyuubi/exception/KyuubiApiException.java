package com.nx1.kyuubi.exception;

/**
 * Thrown when a Kyuubi REST API call returns an unexpected HTTP status code.
 */
public class KyuubiApiException extends RuntimeException {

    private final int statusCode;
    private final String responseBody;

    public KyuubiApiException(int statusCode, String responseBody) {
        super("Kyuubi API error: HTTP " + statusCode + " — " + responseBody);
        this.statusCode   = statusCode;
        this.responseBody = responseBody;
    }

    public KyuubiApiException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode   = -1;
        this.responseBody = null;
    }

    public int    getStatusCode()   { return statusCode; }
    public String getResponseBody() { return responseBody; }
}
