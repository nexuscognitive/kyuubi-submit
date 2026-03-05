package com.nx1.kyuubi.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the JSON response from {@code GET /api/v1/batches/{batchId}}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchStatus {

    @JsonProperty("id")
    private String id;

    @JsonProperty("batchType")
    private String batchType;

    @JsonProperty("state")
    private String state;

    @JsonProperty("appId")
    private String appId;

    @JsonProperty("appState")
    private String appState;

    @JsonProperty("appUrl")
    private String appUrl;

    @JsonProperty("appDiagnostic")
    private String appDiagnostic;

    // ── Accessors ──────────────────────────────────────────────────────────────

    public String getId() { return id; }
    public String getBatchType() { return batchType; }

    public BatchState getState() {
        return BatchState.fromString(state);
    }

    public String getRawState() { return state; }

    public String getAppId() { return appId; }

    /** Application-level state (e.g. SUCCEEDED, FAILED). May be null. */
    public String getAppState() { return appState; }

    public String getAppUrl() { return appUrl; }

    public String getAppDiagnostic() { return appDiagnostic; }

    @Override
    public String toString() {
        return "BatchStatus{id='" + id + "', state='" + state +
               "', appState='" + appState + "', appId='" + appId + "'}";
    }
}
