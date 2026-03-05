/**
 * Kyuubi Batch Submitter — Java library for submitting and monitoring
 * Apache Kyuubi batch jobs via the REST API.
 *
 * <h2>Quick start</h2>
 * <pre>{@code
 * KyuubiClientConfig cfg = KyuubiClientConfig.builder()
 *     .serverUrl("https://kyuubi.internal:10099")
 *     .username("svc-spark")
 *     .password(System.getenv("KYUUBI_PASSWORD"))
 *     .build();
 *
 * SubmitOptions opts = SubmitOptions.builder()
 *     .resource("s3a://my-bucket/jobs/etl.py")
 *     .name("daily-etl")
 *     .conf("spark.executor.memory", "4g")
 *     .build();
 *
 * try (KyuubiClient client = new KyuubiClient(cfg)) {
 *     BatchState state = client.submitAndWait(opts, System.out::println);
 * }
 * }</pre>
 *
 * <p>Main entry points:
 * <ul>
 *   <li>{@link com.nx1.kyuubi.KyuubiClient} — high-level submit / monitor / cancel</li>
 *   <li>{@link com.nx1.kyuubi.model.KyuubiClientConfig} — connection settings</li>
 *   <li>{@link com.nx1.kyuubi.model.SubmitOptions} — job parameters</li>
 *   <li>{@link com.nx1.kyuubi.model.BatchState} — terminal-state enum</li>
 * </ul>
 */
package com.nx1.kyuubi;
