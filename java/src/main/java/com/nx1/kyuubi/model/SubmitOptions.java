package com.nx1.kyuubi.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable value object that describes a Kyuubi batch submission.
 * <p>
 * Build instances with {@link Builder}:
 * <pre>{@code
 * SubmitOptions opts = SubmitOptions.builder()
 *     .resource("/local/path/my_job.py")   // or s3a://…
 *     .name("my-etl-job")
 *     .conf("spark.executor.memory", "4g")
 *     .pyFile("/local/util.py")
 *     .build();
 * }</pre>
 */
public final class SubmitOptions {

    private final String resource;
    private final String className;
    private final String name;
    private final List<String> args;
    private final Map<String, String> conf;
    private final List<String> pyFiles;
    private final List<String> jars;
    private final List<String> files;

    private SubmitOptions(Builder b) {
        this.resource  = Objects.requireNonNull(b.resource,  "resource must not be null");
        this.name      = Objects.requireNonNull(b.name,      "name must not be null");
        this.className = b.className;
        this.args      = Collections.unmodifiableList(new ArrayList<>(b.args));
        this.conf      = Collections.unmodifiableMap(new HashMap<>(b.conf));
        this.pyFiles   = Collections.unmodifiableList(new ArrayList<>(b.pyFiles));
        this.jars      = Collections.unmodifiableList(new ArrayList<>(b.jars));
        this.files     = Collections.unmodifiableList(new ArrayList<>(b.files));
    }

    public String getResource()  { return resource; }
    public String getClassName() { return className; }
    public String getName()      { return name; }
    public List<String> getArgs()    { return args; }
    public Map<String, String> getConf() { return conf; }
    public List<String> getPyFiles() { return pyFiles; }
    public List<String> getJars()    { return jars; }
    public List<String> getFiles()   { return files; }

    // ── Builder ────────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {

        private String resource;
        private String className;
        private String name;
        private final List<String> args    = new ArrayList<>();
        private final Map<String, String> conf = new HashMap<>();
        private final List<String> pyFiles = new ArrayList<>();
        private final List<String> jars    = new ArrayList<>();
        private final List<String> files   = new ArrayList<>();

        /** Path or URI to the main JAR / Python file. */
        public Builder resource(String resource) {
            this.resource = resource;
            return this;
        }

        /** Main class (for JAR jobs; ignored for PySpark). */
        public Builder className(String className) {
            this.className = className;
            return this;
        }

        /** Display name for the batch job. */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /** Append a positional argument passed to the main class / script. */
        public Builder arg(String arg) {
            this.args.add(Objects.requireNonNull(arg));
            return this;
        }

        /** Replace all positional arguments at once. */
        public Builder args(List<String> args) {
            this.args.clear();
            this.args.addAll(args);
            return this;
        }

        /** Add a single Spark configuration key/value pair. */
        public Builder conf(String key, String value) {
            this.conf.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
            return this;
        }

        /** Merge a map of Spark configuration entries. */
        public Builder conf(Map<String, String> conf) {
            this.conf.putAll(conf);
            return this;
        }

        /** Add a Python dependency file (local path or remote URI). */
        public Builder pyFile(String path) {
            this.pyFiles.add(Objects.requireNonNull(path));
            return this;
        }

        /** Add multiple Python dependency files. */
        public Builder pyFiles(List<String> paths) {
            this.pyFiles.addAll(paths);
            return this;
        }

        /** Add a JAR dependency (local path or remote URI). */
        public Builder jar(String path) {
            this.jars.add(Objects.requireNonNull(path));
            return this;
        }

        /** Add multiple JAR dependencies. */
        public Builder jars(List<String> paths) {
            this.jars.addAll(paths);
            return this;
        }

        /** Add a file to distribute to executors (local path or remote URI). */
        public Builder file(String path) {
            this.files.add(Objects.requireNonNull(path));
            return this;
        }

        /** Add multiple files to distribute to executors. */
        public Builder files(List<String> paths) {
            this.files.addAll(paths);
            return this;
        }

        public SubmitOptions build() { return new SubmitOptions(this); }
    }
}
