package com.nx1.kyuubi.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Represents the JSON response from {@code GET /api/v1/batches/{batchId}/localLog}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchLogResponse {

    @JsonProperty("logRowSet")
    private List<String> logRowSet;

    @JsonProperty("total")
    private int total;

    public List<String> getLogRowSet() {
        return logRowSet != null ? logRowSet : Collections.emptyList();
    }

    public int getTotal() { return total; }
}
