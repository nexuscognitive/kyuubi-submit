package com.nx1.kyuubi.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Request body for {@code POST /api/v1/batches}.
 * <p>
 * Fields annotated with {@link JsonInclude} {@code NON_NULL} are omitted from
 * serialization when null, keeping the payload clean.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BatchRequest {

    @JsonProperty("batchType")
    private final String batchType;

    @JsonProperty("resource")
    private String resource;

    @JsonProperty("className")
    private String className;

    @JsonProperty("name")
    private final String name;

    @JsonProperty("args")
    private List<String> args;

    @JsonProperty("conf")
    private Map<String, String> conf;

    /**
     * Maps a Spark conf key (e.g. {@code spark.submit.pyFiles}) to a
     * comma-separated list of <em>filenames</em> (not full paths) that Kyuubi
     * should bind after uploading. Kyuubi appends the temp-file paths to any
     * existing value in {@code conf} for the same key.
     */
    @JsonProperty("extraResourcesMap")
    private Map<String, String> extraResourcesMap;

    // ── Constructor ────────────────────────────────────────────────────────────

    public BatchRequest(BatchType batchType, String name) {
        this.batchType = batchType.name();
        this.name = name;
    }

    // ── Setters ────────────────────────────────────────────────────────────────

    public BatchRequest resource(String resource) {
        this.resource = resource;
        return this;
    }

    public BatchRequest className(String className) {
        this.className = className;
        return this;
    }

    public BatchRequest args(List<String> args) {
        this.args = args;
        return this;
    }

    public BatchRequest conf(Map<String, String> conf) {
        this.conf = conf;
        return this;
    }

    public BatchRequest extraResourcesMap(Map<String, String> extraResourcesMap) {
        this.extraResourcesMap = extraResourcesMap;
        return this;
    }

    // ── Getters ────────────────────────────────────────────────────────────────

    public String getBatchType() { return batchType; }
    public String getResource() { return resource; }
    public String getClassName() { return className; }
    public String getName() { return name; }
    public List<String> getArgs() { return args; }
    public Map<String, String> getConf() { return conf; }
    public Map<String, String> getExtraResourcesMap() { return extraResourcesMap; }
}
