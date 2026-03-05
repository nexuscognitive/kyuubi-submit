package com.nx1.kyuubi.model;

/**
 * Possible states reported by the Kyuubi REST API for a batch job.
 */
public enum BatchState {

    PENDING,
    RUNNING,
    FINISHED,
    ERROR,
    CANCELLED,
    UNKNOWN;

    /** True when the batch has reached a terminal state. */
    public boolean isTerminal() {
        return this == FINISHED || this == ERROR || this == CANCELLED;
    }

    public static BatchState fromString(String value) {
        if (value == null) return UNKNOWN;
        try {
            return BatchState.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
