/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSDelegationToken;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticationHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in real HTTP benchmark. KMS, ZooKeeper, and the load generator run in
 * separate JVMs. No storage calls or authentication results are mocked.
 */
public class TestKMSHttpBenchmark {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String KEY = "benchmark-key";
  private static final String TOKEN_PATH =
      "/benchmark/ZKDTSMRoot/ZKDTSMTokensRoot";
  private static final double LOG_BUCKET = Math.log(1.01);

  /** Seeded shuffled blocks ensure exact ratios across submitted operations. */
  static final class Schedule {
    private final boolean[] create = new boolean[100];
    private long next;
    private final long deadline;

    Schedule(int createPercent, long deadline, long seed) {
      if (createPercent < 0 || createPercent > 100) {
        throw new IllegalArgumentException("Invalid create percentage");
      }
      List<Boolean> block = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        block.add(i < createPercent);
      }
      Collections.shuffle(block, new Random(seed));
      for (int i = 0; i < 100; i++) {
        create[i] = block.get(i);
      }
      this.deadline = deadline;
    }

    synchronized int next() {
      // Finish submitting the current block at cutoff, then drain. Report
      // these extra submissions separately; never count late completions as
      // measurement-window goodput.
      if (System.nanoTime() >= deadline && next % 100 == 0) {
        return -1;
      }
      return create[(int) (next++ % 100)] ? 1 : 0;
    }
  }

  /** Thread-confined latency histogram, approximately 1% bucket precision. */
  static final class Operation {
    private final long[] buckets = new long[2400];
    private long submitted;
    private long submittedAfterCutoff;
    private long successfulInWindow;
    private long successfulAfterCutoff;
    private long errors;
    private long latencyCount;
    private long maxNanos;
    private final Map<String, Long> errorTypes = new LinkedHashMap<>();

    void record(long start, long end, long deadline, String error) {
      submitted++;
      if (start >= deadline) {
        submittedAfterCutoff++;
      } else {
        long nanos = end - start;
        int bucket = (int) Math.ceil(Math.log1p(nanos / 1000.0) / LOG_BUCKET);
        buckets[Math.min(bucket, buckets.length - 1)]++;
        latencyCount++;
        maxNanos = Math.max(maxNanos, nanos);
      }
      if (error != null) {
        errors++;
        errorTypes.merge(error, 1L, Long::sum);
      } else if (end < deadline) {
        successfulInWindow++;
      } else {
        successfulAfterCutoff++;
      }
    }

    void add(Operation other) {
      submitted += other.submitted;
      submittedAfterCutoff += other.submittedAfterCutoff;
      successfulInWindow += other.successfulInWindow;
      successfulAfterCutoff += other.successfulAfterCutoff;
      errors += other.errors;
      latencyCount += other.latencyCount;
      maxNanos = Math.max(maxNanos, other.maxNanos);
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] += other.buckets[i];
      }
      other.errorTypes.forEach((key, value) -> errorTypes.merge(key, value, Long::sum));
    }

    double percentile(double fraction) {
      if (latencyCount == 0) {
        return 0;
      }
      long wanted = (long) Math.ceil(latencyCount * fraction);
      long count = 0;
      for (int i = 0; i < buckets.length; i++) {
        count += buckets[i];
        if (count >= wanted) {
          return Math.expm1(i * LOG_BUCKET) / 1000.0;
        }
      }
      throw new IllegalStateException("Missing latency samples");
    }

    Map<String, Object> result(double seconds) {
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("submitted", submitted);
      result.put("submittedAfterCutoff", submittedAfterCutoff);
      result.put("successfulInWindow", successfulInWindow);
      result.put("successfulAfterCutoff", successfulAfterCutoff);
      result.put("errors", errors);
      result.put("errorTypes", errorTypes);
      result.put("successfulRequestsPerSecond", successfulInWindow / seconds);
      result.put("latencySamples", latencyCount);
      result.put("p50Millis", percentile(0.50));
      result.put("p95Millis", percentile(0.95));
      result.put("p99Millis", percentile(0.99));
      result.put("maxMillis", maxNanos / 1_000_000.0);
      return result;
    }
  }

  @Test
  public void testRequestMixes() {
    for (int percent : new int[]{0, 1, 5, 10}) {
      Schedule schedule = new Schedule(percent, Long.MAX_VALUE, 42);
      int creates = 0;
      for (int i = 0; i < 10_000; i++) {
        creates += schedule.next();
      }
      assertEquals(percent * 100, creates);
    }
    assertEquals(-1, new Schedule(10, 0, 42).next());
  }

  @Test
  public void testLatencyAccounting() {
    Operation op = new Operation();
    op.record(0, 1_000_000, 2_000_000, null);
    op.record(1_000_000, 3_000_000, 2_000_000, null);
    op.record(3_000_000, 4_000_000, 2_000_000, "HTTP_500");
    assertEquals(1, op.successfulInWindow);
    assertEquals(1, op.successfulAfterCutoff);
    assertEquals(1, op.errors);
    assertEquals(1, op.submittedAfterCutoff);
    assertEquals(2, op.latencyCount);
    assertTrue(op.percentile(0.99) >= 2.0);
    assertTrue(op.percentile(0.99) < 2.03);
  }

  @Test
  @Timeout(value = 4, unit = TimeUnit.HOURS)
  @EnabledIfSystemProperty(named = "kms.benchmark", matches = "true")
  public void benchmark() throws Exception {
    Path output = new File(System.getProperty("kms.benchmark.output",
        "target/kms-http-benchmark")).toPath().toAbsolutePath();
    Files.createDirectories(output);
    List<Map<String, Object>> results = new ArrayList<>();
    int repetitions = positive("kms.benchmark.repetitions", 3);
    int warmup = positive("kms.benchmark.warmup.seconds", 5);
    int seconds = positive("kms.benchmark.seconds", 15);
    for (int repetition = 1; repetition <= repetitions; repetition++) {
      for (int concurrency : integers("kms.benchmark.concurrency", "32")) {
        for (int percent : integers("kms.benchmark.create.percent", "0,1,5,10")) {
          Path directory = Files.createTempDirectory(output,
              "r" + repetition + "-c" + concurrency + "-p" + percent + "-");
          Map<String, Object> result = scenario(directory, percent, concurrency,
              warmup, seconds);
          result.put("repetition", repetition);
          result.put("label", System.getProperty("kms.benchmark.label", "working-tree"));
          results.add(result);
          write(output.resolve("results.json"), results);
          System.out.println("KMS_HTTP_BENCHMARK " + JSON.writeValueAsString(result));
          assertEquals(Boolean.TRUE, result.get("valid"), "Invalid run: " + directory);
        }
      }
    }
  }

  private static int positive(String name, int fallback) {
    int value = Integer.getInteger(name, fallback);
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  private static int[] integers(String name, String fallback) {
    return Arrays.stream(System.getProperty(name, fallback).split(","))
        .map(String::trim).mapToInt(Integer::parseInt).toArray();
  }

  private static Map<String, Object> scenario(Path directory, int percent,
      int concurrency, int warmup, int seconds) throws Exception {
    if (concurrency <= 0 || percent < 0 || percent > 100) {
      throw new IllegalArgumentException("Invalid concurrency or percentage");
    }
    try (Child zk = new Child("zk", directory, "")) {
      String connect = zk.ready.get("endpoint").asText();
      try (Child kms = new Child("kms", directory, connect);
           CuratorFramework curator = CuratorFrameworkFactory.newClient(connect,
               new RetryOneTime(100));
           PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager()) {
        curator.start();
        pool.setMaxTotal(concurrency + 4);
        pool.setDefaultMaxPerRoute(concurrency + 4);
        RequestConfig config = RequestConfig.custom().setConnectTimeout(5000)
            .setConnectionRequestTimeout(5000).setSocketTimeout(15000).build();
        try (CloseableHttpClient client = HttpClients.custom()
            .setConnectionManager(pool).setDefaultRequestConfig(config)
            .disableCookieManagement().disableAutomaticRetries()
            .disableRedirectHandling().build()) {
          String base = kms.ready.get("endpoint").asText() + "/v1/";
          HttpPost createKey = new HttpPost(base + "keys?user.name=benchmark");
          createKey.setHeader("Content-Type", "application/json");
          createKey.setEntity(new StringEntity("{\"name\":\"" + KEY
              + "\",\"cipher\":\"AES/CTR/NoPadding\",\"length\":128}",
              StandardCharsets.UTF_8));
          require(client, createKey, 201);
          URI create = URI.create(base
              + "?op=GETDELEGATIONTOKEN&renewer=benchmark&user.name=benchmark");
          URI verify = URI.create(base + "key/" + KEY + "/_metadata");
          String[] tokens = new String[16];
          for (int i = 0; i < tokens.length; i++) {
            tokens[i] = token(require(client, new HttpGet(create), 200));
          }
          JsonNode before = kms.snapshot();
          HttpGet valid = verifyRequest(verify, tokens[0]);
          assertEquals(KEY, require(client, valid, 200).path("name").asText());
          require(client, new HttpGet(create), 200);
          JsonNode after = kms.snapshot();
          assertEquals(1, after.path("verifyRequests").asLong()
              - before.path("verifyRequests").asLong());
          assertEquals(1, after.path("createRequests").asLong()
              - before.path("createRequests").asLong());
          assertTrue(after.path("managerClass").asText().endsWith("$ZKSecretManager"));
          Token<?> invalid = new Token<>();
          invalid.decodeFromUrlString(tokens[0]);
          invalid.setPassword(new byte[32]);
          require(client, verifyRequest(verify, invalid.encodeToUrlString()), 403);
          assertTrue(status(client, new HttpGet(verify)) != 200,
              "Metadata must require authentication");
          kms.command('D'); // Disable diagnostic counters for measured traffic.
          phase(client, create, verify, tokens, percent, concurrency, warmup);
          int tokensBefore = curator.getChildren().forPath(TOKEN_PATH).size();
          JsonNode kmsBefore = kms.snapshot();
          JsonNode zkBefore = zk.snapshot();
          Map<String, Object> clientBefore = jvmStats();
          Phase measurement = phase(client, create, verify, tokens,
              percent, concurrency, seconds);
          Map<String, Object> clientAfter = jvmStats();
          JsonNode kmsAfter = kms.snapshot();
          JsonNode zkAfter = zk.snapshot();
          int tokensAfter = curator.getChildren().forPath(TOKEN_PATH).size();
          Map<String, Object> result = new LinkedHashMap<>();
          result.put("verifyPercent", 100 - percent);
          result.put("createPercent", percent);
          result.put("concurrency", concurrency);
          result.put("warmupSeconds", warmup);
          result.put("measurementSeconds", seconds);
          result.put("actualElapsedWithDrainSeconds", measurement.elapsed);
          result.put("addedDelayMillis", 0);
          result.put("sequenceBatchSize", 100);
          result.put("authentication", "simple issuance; delegation-token verification");
          result.put("verify", measurement.verify.result(seconds));
          result.put("create", measurement.create.result(seconds));
          result.put("successfulRequestsPerSecond",
              (measurement.verify.successfulInWindow
                  + measurement.create.successfulInWindow) / (double) seconds);
          result.put("tokensBefore", tokensBefore);
          result.put("tokensAfter", tokensAfter);
          result.put("kmsBefore", kmsBefore);
          result.put("kmsAfter", kmsAfter);
          result.put("zkBefore", zkBefore);
          result.put("zkAfter", zkAfter);
          result.put("clientBefore", clientBefore);
          result.put("clientAfter", clientAfter);
          result.put("serverMetadata", kms.ready);
          result.put("directory", directory.toString());
          long submitted = measurement.verify.submitted + measurement.create.submitted;
          boolean validRun = measurement.verify.errors + measurement.create.errors == 0
              && submitted > 0 && measurement.create.submitted * 100 == submitted * percent
              && tokensAfter - tokensBefore == measurement.create.submitted;
          result.put("valid", validRun);
          return result;
        }
      }
    }
  }

  private static final class Phase {
    private final Operation verify = new Operation();
    private final Operation create = new Operation();
    private double elapsed;
  }

  private static Phase phase(CloseableHttpClient client, URI create, URI verify,
      String[] tokens, int percent, int concurrency, int seconds) throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(concurrency);
    CountDownLatch ready = new CountDownLatch(concurrency);
    CountDownLatch start = new CountDownLatch(1);
    long[] timing = new long[2];
    List<Future<Phase>> futures = new ArrayList<>();
    try {
      // The shared schedule is installed before the start latch publishes it.
      Schedule[] schedule = new Schedule[1];
      for (int i = 0; i < concurrency; i++) {
        final int worker = i;
        futures.add(workers.submit(() -> {
          Phase result = new Phase();
          ready.countDown();
          start.await();
          int operation;
          while ((operation = schedule[0].next()) >= 0) {
            long requestStart = System.nanoTime();
            String error = null;
            HttpGet request = operation == 1 ? new HttpGet(create)
                : verifyRequest(verify, tokens[worker % tokens.length]);
            try (CloseableHttpResponse response = client.execute(request)) {
              int code = response.getStatusLine().getStatusCode();
              byte[] body = EntityUtils.toByteArray(response.getEntity());
              if (code != 200) {
                error = "HTTP_" + code;
              } else {
                JsonNode json = JSON.readTree(body);
                if (operation == 1) {
                  token(json);
                } else if (!KEY.equals(json.path("name").asText())) {
                  error = "INVALID_METADATA";
                }
              }
            } catch (Exception e) {
              error = e.getClass().getSimpleName();
            } finally {
              request.releaseConnection();
            }
            (operation == 1 ? result.create : result.verify)
                .record(requestStart, System.nanoTime(), timing[1], error);
          }
          return result;
        }));
      }
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      timing[0] = System.nanoTime();
      timing[1] = timing[0] + TimeUnit.SECONDS.toNanos(seconds);
      schedule[0] = new Schedule(percent, timing[1], 42);
      start.countDown();
      Phase result = new Phase();
      long finishDeadline = timing[1] + TimeUnit.SECONDS.toNanos(120);
      for (Future<Phase> future : futures) {
        Phase local = future.get(Math.max(1, finishDeadline - System.nanoTime()),
            TimeUnit.NANOSECONDS);
        result.verify.add(local.verify);
        result.create.add(local.create);
      }
      result.elapsed = (System.nanoTime() - timing[0]) / 1_000_000_000.0;
      assertEquals(0, result.verify.errors + result.create.errors,
          "HTTP errors: " + result.verify.errorTypes + result.create.errorTypes);
      return result;
    } finally {
      start.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private static HttpGet verifyRequest(URI uri, String token) {
    HttpGet request = new HttpGet(uri);
    request.setHeader(DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER, token);
    return request;
  }

  private static String token(JsonNode json) throws Exception {
    String encoded = json.path("Token").path("urlString").asText();
    Token<?> token = new Token<>();
    token.decodeFromUrlString(encoded);
    if (!KMSDelegationToken.TOKEN_KIND_STR.equals(token.getKind().toString())
        || token.getIdentifier().length == 0 || token.getPassword().length == 0) {
      throw new IllegalStateException("Invalid issued delegation token");
    }
    return encoded;
  }

  private static JsonNode require(CloseableHttpClient client,
      HttpRequestBase request, int expected) throws Exception {
    try (CloseableHttpResponse response = client.execute(request)) {
      assertEquals(expected, response.getStatusLine().getStatusCode(),
          request.getMethod() + " " + request.getURI().getPath());
      return JSON.readTree(EntityUtils.toByteArray(response.getEntity()));
    } finally {
      request.releaseConnection();
    }
  }

  private static int status(CloseableHttpClient client, HttpGet request) throws Exception {
    try (CloseableHttpResponse response = client.execute(request)) {
      EntityUtils.consume(response.getEntity());
      return response.getStatusLine().getStatusCode();
    }
  }

  /** Short diagnostic pass delegates to the real, unmodified handler methods. */
  public static class DiagnosticHandler extends PseudoDelegationTokenAuthenticationHandler {
    private static volatile boolean diagnostic = true;
    private static final AtomicLong VERIFIES = new AtomicLong();
    private static final AtomicLong CREATES = new AtomicLong();
    private static AbstractDelegationTokenSecretManager<?> manager;

    @Override
    public void initTokenManager(Properties config) {
      super.initTokenManager(config);
      try {
        Method getter = DelegationTokenAuthenticationHandler.class
            .getDeclaredMethod("getTokenManager");
        getter.setAccessible(true);
        manager = ((DelegationTokenManager) getter.invoke(this))
            .getDelegationTokenSecretManager();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request,
        HttpServletResponse response) throws java.io.IOException, AuthenticationException {
      if (diagnostic && request.getHeader(
          DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER) != null) {
        VERIFIES.incrementAndGet();
      }
      return super.authenticate(request, response);
    }

    @Override
    public boolean managementOperation(AuthenticationToken token,
        HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, AuthenticationException {
      if (diagnostic && "GETDELEGATIONTOKEN".equals(request.getParameter("op"))) {
        CREATES.incrementAndGet();
      }
      return super.managementOperation(token, request, response);
    }
  }

  private static final class Child implements Closeable {
    private final Process process;
    private final Path directory;
    private final String role;
    private final JsonNode ready;
    private int snapshots;

    Child(String role, Path directory, String endpoint) throws Exception {
      this.directory = directory;
      this.role = role;
      List<String> command = new ArrayList<>();
      command.add(new File(System.getProperty("java.home"), "bin/java").toString());
      command.add("-Xmx768m");
      Path logging = directory.resolve("benchmark-log4j.properties");
      if (!Files.exists(logging)) {
        Files.write(logging, Arrays.asList("log4j.rootLogger=WARN,console",
            "log4j.appender.console=org.apache.log4j.ConsoleAppender",
            "log4j.appender.console.layout=org.apache.log4j.PatternLayout",
            "log4j.appender.console.layout.ConversionPattern=%d %-5p %c - %m%n"),
            StandardCharsets.UTF_8);
      }
      command.add("-Dlog4j.configuration=" + logging.toUri());
      for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
        if (argument.startsWith("--add-opens=")) {
          command.add(argument);
        }
      }
      command.add("-Dzookeeper.4lw.commands.whitelist=mntr");
      command.add("-Dzookeeper.nio.numWorkerThreads=8");
      command.add("-Dzookeeper.nio.numSelectorThreads=1");
      command.add("-cp");
      String classpath = System.getProperty("surefire.test.class.path",
          System.getProperty("java.class.path"));
      String override = System.getProperty("kms.benchmark.common.jar");
      command.add(override == null ? classpath : override + File.pathSeparator + classpath);
      command.add(TestKMSHttpBenchmark.class.getName());
      command.add(role);
      command.add(directory.toString());
      command.add(endpoint);
      process = new ProcessBuilder(command).redirectErrorStream(true)
          .redirectOutput(directory.resolve(role + ".log").toFile()).start();
      JsonNode metadata;
      try {
        metadata = await(directory.resolve(role + "-ready.json"));
      } catch (Exception | AssertionError e) {
        close();
        throw e;
      }
      ready = metadata;
    }

    void command(int command) throws java.io.IOException {
      process.getOutputStream().write(command);
      process.getOutputStream().flush();
    }

    JsonNode snapshot() throws Exception {
      Path file = directory.resolve(role + "-stats-" + snapshots++ + ".json");
      command('S');
      return await(file);
    }

    private JsonNode await(Path file) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (!Files.exists(file)) {
        if (!process.isAlive() || System.nanoTime() > deadline) {
          throw new IllegalStateException("Child failed: " + directory.resolve(role + ".log"));
        }
        Thread.sleep(25);
      }
      return JSON.readTree(file.toFile());
    }

    @Override
    public void close() throws java.io.IOException {
      try {
        if (process.isAlive()) {
          command('Q');
          if (!process.waitFor(20, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new java.io.IOException("Child did not stop: " + role);
          }
          if (process.exitValue() != 0) {
            throw new java.io.IOException("Child exit " + process.exitValue() + ": " + role);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
        throw new java.io.IOException(e);
      } finally {
        process.getOutputStream().close();
      }
    }
  }

  private static void write(Path path, Object value) throws Exception {
    Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
    JSON.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), value);
    Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE);
  }

  private static Map<String, Object> jvmStats() {
    Map<String, Object> result = new LinkedHashMap<>();
    java.lang.management.OperatingSystemMXBean os =
        ManagementFactory.getOperatingSystemMXBean();
    if (os instanceof com.sun.management.OperatingSystemMXBean) {
      result.put("processCpuNanos",
          ((com.sun.management.OperatingSystemMXBean) os).getProcessCpuTime());
    }
    result.put("heapUsedBytes", ManagementFactory.getMemoryMXBean()
        .getHeapMemoryUsage().getUsed());
    result.put("liveThreads", ManagementFactory.getThreadMXBean().getThreadCount());
    long collections = 0;
    long collectionMillis = 0;
    for (java.lang.management.GarbageCollectorMXBean gc :
        ManagementFactory.getGarbageCollectorMXBeans()) {
      collections += Math.max(0, gc.getCollectionCount());
      collectionMillis += Math.max(0, gc.getCollectionTime());
    }
    result.put("gcCollections", collections);
    result.put("gcMillis", collectionMillis);
    result.put("verifyRequests", DiagnosticHandler.VERIFIES.get());
    result.put("createRequests", DiagnosticHandler.CREATES.get());
    if (DiagnosticHandler.manager != null) {
      result.put("managerClass", DiagnosticHandler.manager.getClass().getName());
      result.put("cachedTokens", DiagnosticHandler.manager.getCurrentTokensSize());
    }
    return result;
  }

  /** Entry point for isolated server processes, never for the load generator. */
  public static void main(String[] args) throws Exception {
    String role = args[0];
    Path directory = new File(args[1]).toPath();
    TestingServer zk = null;
    MiniKMS kms = null;
    try {
      String endpoint;
      if ("zk".equals(role)) {
        zk = new TestingServer(-1, directory.resolve("zk-data").toFile(), true);
        endpoint = zk.getConnectString();
      } else {
        Path confDir = Files.createDirectory(directory.resolve("conf"));
        Configuration conf = new Configuration(false);
        conf.set(KMSConfiguration.KEY_PROVIDER_URI,
            "jceks://file@" + new org.apache.hadoop.fs.Path(
                confDir.resolve("kms.keystore").toString()).toUri());
        String prefix = KMSAuthenticationFilter.CONFIG_PREFIX;
        conf.set(prefix + "type", DiagnosticHandler.class.getName());
        conf.setBoolean(prefix + "simple.anonymous.allowed", false);
        conf.set(prefix + ZKDelegationTokenSecretManager.ZK_DTSM_ZK_CONNECTION_STRING, args[2]);
        conf.setBoolean(prefix + DelegationTokenManager.ENABLE_ZK_KEY, true);
        conf.set(prefix + ZKDelegationTokenSecretManager.ZK_DTSM_ZK_AUTH_TYPE, "none");
        conf.set(prefix + ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH, "benchmark");
        conf.setInt(prefix + ZKDelegationTokenSecretManager.ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE, 100);
        conf.setLong(prefix + DelegationTokenManager.UPDATE_INTERVAL, 86400);
        conf.setLong(prefix + DelegationTokenManager.MAX_LIFETIME, 86400);
        conf.setLong(prefix + DelegationTokenManager.RENEW_INTERVAL, 86400);
        conf.setLong(prefix + DelegationTokenManager.REMOVAL_SCAN_INTERVAL, 86400);
        conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, 256);
        for (KMSACLs.Type acl : KMSACLs.Type.values()) {
          conf.set(acl.getAclConfigKey(), "*");
        }
        conf.set(KeyAuthorizationKeyProvider.KEY_ACL + KEY + ".ALL", "*");
        for (String file : new String[]{"kms-site.xml", "kms-acls.xml", "core-site.xml"}) {
          try (Writer writer = Files.newBufferedWriter(confDir.resolve(file),
              StandardCharsets.UTF_8)) {
            conf.writeXml(writer);
          }
        }
        kms = new MiniKMS.Builder().setKmsConfDir(confDir.toFile())
            .setLog4jConfFile(directory.resolve("benchmark-log4j.properties").toUri().toString())
            .build();
        kms.start();
        endpoint = kms.getKMSUrl().toString();
      }
      Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("endpoint", endpoint);
      metadata.put("javaVersion", System.getProperty("java.version"));
      metadata.put("httpServerMaxThreads", 256);
      metadata.put("maxHeapBytes", Runtime.getRuntime().maxMemory());
      metadata.put("availableProcessors", Runtime.getRuntime().availableProcessors());
      metadata.put("commonLocation", AbstractDelegationTokenSecretManager.class
          .getProtectionDomain().getCodeSource().getLocation().toString());
      metadata.put("abstractManagerSha256", classHash(AbstractDelegationTokenSecretManager.class));
      metadata.put("zkManagerSha256", classHash(ZKDelegationTokenSecretManager.class));
      write(directory.resolve(role + "-ready.json"), metadata);
      int snapshot = 0;
      int command;
      while ((command = System.in.read()) != -1 && command != 'Q') {
        if (command == 'S') {
          Map<String, Object> stats = jvmStats();
          if ("zk".equals(role)) {
            stats.put("serverMetrics", zooKeeperStats(endpoint));
            try (java.util.stream.Stream<Path> files = Files.walk(directory.resolve("zk-data"))) {
              stats.put("dataFileBytes", files.filter(Files::isRegularFile)
                  .mapToLong(path -> path.toFile().length()).sum());
            }
          }
          write(directory.resolve(role + "-stats-" + snapshot++ + ".json"), stats);
        } else if (command == 'D') {
          DiagnosticHandler.diagnostic = false;
        }
      }
    } finally {
      if (kms != null) {
        kms.stop();
      }
      if (zk != null) {
        zk.close();
      }
    }
  }

  private static Map<String, Object> zooKeeperStats(String endpoint) throws Exception {
    Map<String, Object> result = new LinkedHashMap<>();
    String[] address = endpoint.split(":");
    try (java.net.Socket socket = new java.net.Socket(address[0],
        Integer.parseInt(address[1]))) {
      socket.setSoTimeout(5000);
      socket.getOutputStream().write("mntr".getBytes(StandardCharsets.UTF_8));
      socket.getOutputStream().flush();
      try (java.io.BufferedReader reader = new java.io.BufferedReader(
          new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\\s+", 2);
          if (fields.length == 2 && fields[0].startsWith("zk_")) {
            try {
              result.put(fields[0], Double.parseDouble(fields[1]));
            } catch (NumberFormatException e) {
              result.put(fields[0], fields[1]);
            }
          }
        }
      }
    }
    if (!result.containsKey("zk_avg_latency")) {
      throw new IllegalStateException("ZooKeeper mntr statistics unavailable");
    }
    return result;
  }

  private static String classHash(Class<?> type) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream input = type.getResourceAsStream(type.getSimpleName() + ".class")) {
      byte[] buffer = new byte[8192];
      int size;
      while ((size = input.read(buffer)) != -1) {
        digest.update(buffer, 0, size);
      }
    }
    StringBuilder hex = new StringBuilder();
    for (byte value : digest.digest()) {
      hex.append(String.format("%02x", value & 0xff));
    }
    return hex.toString();
  }
}
