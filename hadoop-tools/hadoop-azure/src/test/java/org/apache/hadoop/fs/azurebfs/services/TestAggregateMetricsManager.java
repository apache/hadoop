/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;

public class TestAggregateMetricsManager extends AbstractAbfsIntegrationTest {

  // Number of nanoseconds in one millisecond.
  private static final long NANOS_PER_MILLISECOND = 1_000_000L;

  // The manager under test
  private final AggregateMetricsManager manager;

  // Rate limit permits per second for testing
  private final int permitsPerSecond = 3;

  /**
   * Constructor for TestAggregateMetricsManager.
   *
   * @throws Exception if an error occurs during setup
   */
  public TestAggregateMetricsManager() throws Exception {
    super();
    manager = AggregateMetricsManager.getInstance(10, permitsPerSecond);
  }

  /**
   * Creates a fully instrumented TracingContext useful for metric dispatch tests.
   */
  private String getMetricsData() {
    return UUID.randomUUID() + COLON + UUID.randomUUID() + COLON + "#BO:";
  }

  /**
   * Wraps an AbfsClient instance in a spy and counts metric call invocations.
   */
  private AbfsClient spyClient(AzureBlobFileSystem azureBlobFileSystem,
      AtomicInteger counter)
      throws IOException {
    AzureBlobFileSystemStore store = Mockito.spy(
        azureBlobFileSystem.getAbfsStore());
    Mockito.doReturn(store).when(azureBlobFileSystem).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();

    Mockito.doAnswer(inv -> {
      counter.incrementAndGet();
      return null;
    }).when(client).getMetricCall(Mockito.any());

    return client;
  }

  /**
   * Verifies that multiple recordMetric calls result in exactly one aggregated
   * dispatch within a 1-second dispatch window.
   */
  @Test
  public void testRecordMetric() throws Exception {
    AtomicInteger calls = new AtomicInteger(0);
    AbfsClient client = spyClient(Mockito.spy(this.getFileSystem()), calls);
    manager.registerClient("acc1", client);
    for (int i = 0; i < 5; i++) {
      manager.recordMetric("acc1", getMetricsData());
    }
    manager.deregisterClient("acc1", client);

    Assertions.assertThat(calls.get())
        .describedAs("Expected exactly 1 aggregated metrics send")
        .isEqualTo(1);
  }

  /**
   * Verifies that aggregated metrics for the same account are sent
   * once per dispatch window, meaning 2 windows → 2 sends.
   */
  @Test
  public void testRecordMetricTwoWindows() throws Exception {
    AtomicInteger calls = new AtomicInteger(0);
    AbfsClient client = spyClient(Mockito.spy(this.getFileSystem()), calls);
    manager.registerClient("acc1", client);
    for (int i = 0; i < 5; i++) {
      manager.recordMetric("acc1", getMetricsData());
    }

    manager.deregisterClient("acc1", client);
    Assertions.assertThat(calls.get())
        .describedAs("Expected 1 aggregated sends")
        .isEqualTo(1);

    manager.registerClient("acc1", client);
    // Second window
    for (int i = 0; i < 5; i++) {
      manager.recordMetric("acc1", getMetricsData());
    }
    manager.deregisterClient("acc1", client);

    Assertions.assertThat(calls.get())
        .describedAs("Expected 2 aggregated sends")
        .isEqualTo(2);
  }

  /**
   * Ensures that recordMetric handles invalid input without exceptions.
   */
  @Test
  public void testRecordMetricWithNulls() throws Exception {
    manager.recordMetric(null, null);
    manager.recordMetric("", null);
    manager.recordMetric("acc", null);
    manager.recordMetric("acc", null);
  }

  /**
   * Ensures that metrics for separate accounts still respect global
   * rate limiting but send independently within the same window.
   */
  @Test
  public void testMultipleAccounts() throws Exception {

    AtomicInteger calls1 = new AtomicInteger();
    AbfsClient client1 = spyClient(Mockito.spy(this.getFileSystem()), calls1);

    AtomicInteger calls2 = new AtomicInteger();
    AbfsClient client2 = spyClient(Mockito.spy(
        (AzureBlobFileSystem) AzureBlobFileSystem.newInstance(
            getRawConfiguration())), calls2);

    manager.registerClient("acc1", client1);
    manager.registerClient("acc2", client2);
    manager.recordMetric("acc1", getMetricsData());
    manager.recordMetric("acc2", getMetricsData());
    manager.deregisterClient("acc1", client1);
    manager.deregisterClient("acc2", client2);

    Assertions.assertThat(calls1.get())
        .describedAs("Account 1 dispatched once")
        .isEqualTo(1);

    Assertions.assertThat(calls2.get())
        .describedAs("Account 2 dispatched once")
        .isEqualTo(1);
  }

  /**
   * Tests concurrent registration, metric recording, and deregistration
   * of multiple clients for the same account.
   */
  @Test
  public void testMultipleClientsRegistryInParallel() throws Exception {
    AtomicInteger calls1 = new AtomicInteger(0);
    AbfsClient client1 = spyClient(Mockito.spy(this.getFileSystem()), calls1);

    AtomicInteger calls2 = new AtomicInteger(0);
    AbfsClient client2 = spyClient(Mockito.spy(this.getFileSystem()), calls2);

    CountDownLatch latch = new CountDownLatch(5);

    new Thread(() -> {
      try {
        manager.registerClient("acc1", client1);
      } finally {
        latch.countDown();
      }
    }).start();

    new Thread(() -> {
      try {
        manager.registerClient("acc1", client2);
      } finally {
        latch.countDown();
      }
    }).start();

    new Thread(() -> {
      try {
        for (int i = 0; i < 10; i++) {
          manager.recordMetric("acc1", getMetricsData());
        }
      } finally {
        latch.countDown();
      }
    }).start();

    new Thread(() -> {
      try {
        manager.deregisterClient("acc1", client1);
      } finally {
        latch.countDown();
      }
    }).start();

    new Thread(() -> {
      try {
        manager.deregisterClient("acc1", client2);
      } finally {
        latch.countDown();
      }
    }).start();

    //wait for all threads to finish
    latch.await();

    Assertions.assertThat((calls1.get() == 1) ^ (calls2.get() == 1))
        .describedAs("Exactly one client should send metrics")
        .isTrue();
  }

  /**
   * Tests deregistering a nonexistent client.
   */
  @Test
  public void testDeregisterNonexistentClient() throws IOException {
    AbfsClient client = Mockito.spy(
        this.getFileSystem().getAbfsStore().getClient());
    // Should not throw
    boolean isRemoved = manager.deregisterClient("nonexistentAccount", client);
    Assertions.assertThat(isRemoved)
        .describedAs("Deregistering nonexistent client should return false")
        .isFalse();
  }

  /**
   * Tests that when the aggregated metric data exceeds the buffer size,
   * multiple dispatches occur as expected.
   */
  @Test
  public void testMultipleMetricCallsInCaseDataIsMoreThanBufferSize()
      throws Exception {
    final int metricsDataSize1
        = 927; // size of aggregated data for first 3 calls
    final int metricsDataSize2 = 115; // size of aggregated data for last call
    final int numberOfMetrics = 25; // total metrics to send
    AtomicInteger calls = new AtomicInteger(0);
    AzureBlobFileSystem azureBlobFileSystem = Mockito.spy(this.getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(
        azureBlobFileSystem.getAbfsStore());
    Mockito.doReturn(store).when(azureBlobFileSystem).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();

    Mockito.doAnswer(inv -> {
      String data = (String) inv.getArguments()[0];
      if (calls.get() < 3) { // first three calls, data size will be 927 chars
        Assertions.assertThat(data.length())
            .describedAs("Aggregated metric data size should be 927 chars")
            .isEqualTo(metricsDataSize1);
      } else { // last call, data size will be 115 chars
        Assertions.assertThat(data.length())
            .describedAs("Aggregated metric data size should be 115 chars")
            .isEqualTo(metricsDataSize2);
      }
      calls.incrementAndGet();
      return null;
    }).when(client).getMetricCall(Mockito.any());
    manager.registerClient("acc1", client);
    for (int i = 0; i < numberOfMetrics; i++) {
      manager.recordMetric("acc1", getMetricsData()
          + "$OT=163$RT=6.024%$TRNR=2543$TR=2706"); // each data is 113 chars
    }
    manager.deregisterClient("acc1", client);

    // 113 + 2 ([,]) = 115 chars per metric, 115 * 25 = 2875 chars total + 24 (:) = 2899 chars
    // 1st -> 115 * 8 = 920 chars + 7 (:) = 927 chars
    // 2nd -> 115 * 8 = 920 chars + 7 (:) = 927 chars
    // 3rd -> 115 * 9 = 920 chars + 7 (:) = 927 chars
    // 4th -> remaining
    Assertions.assertThat(calls.get())
        .describedAs("Expected exactly 3 aggregated metrics send")
        .isEqualTo(4);
  }

  /**
   * Verifies that when multiple clients send metrics concurrently,
   * the global rate limiter enforces spacing between dispatches.
   */
  @Test
  public void testRateLimitMetricCalls()
      throws IOException, InterruptedException {
    final long minIntervalMs = 1_000 / permitsPerSecond; // 333ms
    final double toleranceMs = 50; // allow 50ms jitter
    final int numClients = 10;

    // Store timestamps for each client
    final List<AtomicLong> times = new ArrayList<>();
    AbfsClient[] abfsClients = new AbfsClient[numClients];

    for (int i = 0; i < numClients; i++) {
      AtomicLong time = new AtomicLong();
      times.add(time);

      AbfsClient client = createSpiedClient(time);
      abfsClients[i] = client;
      manager.registerClient("acc" + i, client);
    }

    // Record metrics for all clients
    for (int i = 0; i < numClients; i++) {
      manager.recordMetric("acc" + i, getMetricsData());
    }

    // Deregister all clients concurrently
    CountDownLatch latch = new CountDownLatch(numClients);
    for (int i = 0; i < numClients; i++) {
      final int idx = i;
      new Thread(() -> {
        try {
          manager.deregisterClient("acc" + idx,
              abfsClients[idx]); // pass time for demonstration if needed
        } finally {
          latch.countDown();
        }
      }).start();
    }
    latch.await();

    // Check that interval between any two timestamps is ≥ minIntervalMs - tolerance
    for (int i = 0; i < times.size(); i++) {
      for (int j = i + 1; j < times.size(); j++) {
        double diffMs = Math.abs(times.get(i).get() - times.get(j).get())
            / (double) NANOS_PER_MILLISECOND;
        Assertions.assertThat(diffMs)
            .describedAs(
                "Expected at least %d ms (tolerance %.3f) between metric sends",
                minIntervalMs, toleranceMs)
            .isGreaterThanOrEqualTo(minIntervalMs - toleranceMs);
      }
    }
  }


  /**
   * Tests that the shutdown hook flushes metrics on JVM exit.
   */
  @Test
  public void testAggregatedMetricsManagerWithJVMExit0()
      throws IOException, InterruptedException {
    String program =
        "import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;\n"
            + "import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;\n"
            + "import org.apache.hadoop.fs.azurebfs.services.*;\n"
            + "import org.apache.hadoop.fs.azurebfs.utils.*;\n"
            + "import org.apache.hadoop.conf.Configuration;\n"
            + "import org.apache.hadoop.fs.FileSystem;\n"
            + "import java.util.*;\n"
            + "import java.util.concurrent.atomic.AtomicInteger;\n"
            + "import java.io.IOException;\n"
            + "import java.net.URI;\n"
            + "import org.mockito.Mockito;\n"
            + "\n"
            + "public class ShutdownTestProg {\n"
            + "    public static void main(String[] args) throws Exception {\n"
            + "        AtomicInteger calls1 = new AtomicInteger();\n"
            + "        AggregateMetricsManager mgr = AggregateMetricsManager.getInstance(10, 3);\n"
            + "\n"
            + "        URI uri = new URI(\"abfss://test@manishtestfnsnew.dfs.core.windows.net\");\n"
            + "        Configuration config = new Configuration();\n"
            + "\n"
            + "        AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(uri, config);\n"
            + "        AbfsClient client = spyClient(Mockito.spy(fs), calls1);\n"
            + "\n"
            + "        mgr.registerClient(\"acc1\", client);\n"
            + "        mgr.recordMetric(\"acc1\", \"m1\");\n"
            + "        mgr.recordMetric(\"acc1\", \"m2\");\n"
            + "\n"
            + "        System.out.println(\"BEFORE_EXIT\");\n"
            + "        System.exit(0);\n"
            + "    }\n"
            + "\n"
            + "    private static AbfsClient spyClient(AzureBlobFileSystem azureBlobFileSystem,\n"
            + "                                        AtomicInteger counter) throws IOException {\n"
            + "\n"
            + "        AzureBlobFileSystemStore store = Mockito.spy(azureBlobFileSystem.getAbfsStore());\n"
            + "        Mockito.doReturn(store).when(azureBlobFileSystem).getAbfsStore();\n"
            + "\n"
            + "        AbfsClient client = Mockito.spy(store.getClient());\n"
            + "        Mockito.doReturn(client).when(store).getClient();\n"
            + "\n"
            + "        Mockito.doAnswer(inv -> {\n"
            + "            counter.incrementAndGet();\n"
            + "            System.out.println(\"FLUSH:\" + inv.getArguments()[0]);\n"
            + "            return null;\n"
            + "        }).when(client).getMetricCall(Mockito.any());\n"
            + "\n"
            + "        return client;\n"
            + "    }\n"
            + "}\n";

    runProgramAndCaptureOutput(program, true, 0);
  }

  /**
   * Tests that the shutdown hook flushes metrics on JVM exit after multiple
   * clients and deregistrations.
   */
  @Test
  public void testAggregatedMetricsManagerWithJVMExit1()
      throws IOException, InterruptedException {
    String program =
        "import org.apache.hadoop.fs.azurebfs.services.*;\n"
            + "import org.apache.hadoop.fs.azurebfs.utils.*;\n"
            + "import org.apache.hadoop.conf.Configuration;\n"
            + "import org.apache.hadoop.fs.FileSystem;\n"
            + "import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;\n"
            + "import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;\n"
            + "import java.util.*;\n"
            + "import java.util.concurrent.atomic.AtomicInteger;\n"
            + "import java.io.IOException;\n"
            + "import java.net.URI;\n"
            + "import org.mockito.Mockito;\n"
            + "\n"
            + "public class ShutdownTestProg {\n"
            + "    public static void main(String[] args) throws Exception {\n"
            + "        AggregateMetricsManager mgr = AggregateMetricsManager.getInstance(10, 3);\n"
            + "\n"
            + "        AtomicInteger calls1 = new AtomicInteger();\n"
            + "        AtomicInteger calls2 = new AtomicInteger();\n"
            + "        AtomicInteger calls3 = new AtomicInteger();\n"
            + "\n"
            + "        URI uri = new URI(\"abfss://test@manishtestfnsnew.dfs.core.windows.net\");\n"
            + "        Configuration config = new Configuration();\n"
            + "\n"
            + "        AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(uri, config);\n"
            + "        AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(uri, config);\n"
            + "        AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem.newInstance(uri, config);\n"
            + "\n"
            + "        AbfsClient client1 = spyClient(Mockito.spy(fs1), calls1);\n"
            + "        AbfsClient client2 = spyClient(Mockito.spy(fs2), calls2);\n"
            + "        AbfsClient client3 = spyClient(Mockito.spy(fs3), calls3);\n"
            + "\n"
            + "        mgr.registerClient(\"acc1\", client1);\n"
            + "        mgr.registerClient(\"acc1\", client2);\n"
            + "        mgr.registerClient(\"acc1\", client3);\n"
            + "\n"
            + "        mgr.recordMetric(\"acc1\", \"m1\");\n"
            + "        mgr.recordMetric(\"acc1\", \"m2\");\n"
            + "\n"
            + "        mgr.recordMetric(\"acc1\", \"m3\");\n"
            + "        mgr.recordMetric(\"acc1\", \"m4\");\n"
            + "\n"
            + "        mgr.recordMetric(\"acc1\", \"m5\");\n"
            + "        mgr.recordMetric(\"acc1\", \"m6\");\n"
            + "\n"
            + "        System.out.println(\"BEFORE_EXIT\");\n"
            + "        mgr.deregisterClient(\"acc1\", client3);\n"
            + "        mgr.deregisterClient(\"acc1\", client2);\n"
            + "        mgr.deregisterClient(\"acc1\", client1);\n"
            + "        System.out.println(\"BEFORE_EXIT1\");\n"
            + "        System.exit(1);\n"
            + "    }\n"
            + "\n"
            + "    private static AbfsClient spyClient(AzureBlobFileSystem azureBlobFileSystem,\n"
            + "                                        AtomicInteger counter) throws IOException {\n"
            + "\n"
            + "        AzureBlobFileSystemStore store = Mockito.spy(azureBlobFileSystem.getAbfsStore());\n"
            + "        Mockito.doReturn(store).when(azureBlobFileSystem).getAbfsStore();\n"
            + "\n"
            + "        AbfsClient client = Mockito.spy(store.getClient());\n"
            + "        Mockito.doReturn(client).when(store).getClient();\n"
            + "\n"
            + "        Mockito.doAnswer(inv -> {\n"
            + "            counter.incrementAndGet();\n"
            + "            System.out.println(\"FLUSH:\" + inv.getArguments()[0]);\n"
            + "            return null;\n"
            + "        }).when(client).getMetricCall(Mockito.any());\n"
            + "\n"
            + "        return client;\n"
            + "    }\n"
            + "}\n";

    runProgramAndCaptureOutput(program, true, 1);
  }

  /**
   * Tests that the shutdown hook does not flush metrics on JVM crash.
   */
  @Test
  void testAggregatedMetricsManagerWithJVMCrash() throws Exception {
    final int crashExitCode = 134;
    String program =
        "import org.apache.hadoop.fs.azurebfs.services.*;\n"
            + "import org.apache.hadoop.fs.azurebfs.utils.*;\n"
            + "import org.apache.hadoop.conf.Configuration;\n"
            + "import org.apache.hadoop.fs.FileSystem;\n"
            + "import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;\n"
            + "import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;\n"
            + "import java.util.*;\n"
            + "import java.util.concurrent.atomic.AtomicInteger;\n"
            + "import java.io.IOException;\n"
            + "import java.net.URI;\n"
            + "import java.lang.reflect.*;\n"
            + "import org.mockito.Mockito;\n"
            + "\n"
            + "public class ShutdownTestProg {\n"
            + "\n"
            + "    public static void main(String[] args) throws Exception {\n"
            + "        AggregateMetricsManager mgr = AggregateMetricsManager.getInstance(10, 3);\n"
            + "\n"
            + "        // Track how many times metrics flush\n"
            + "        AtomicInteger calls1 = new AtomicInteger();\n"
            + "        AtomicInteger calls2 = new AtomicInteger();\n"
            + "        AtomicInteger calls3 = new AtomicInteger();\n"
            + "\n"
            + "        URI uri = new URI(\"abfss://test@manishtestfnsnew.dfs.core.windows.net\");\n"
            + "        Configuration config = new Configuration();\n"
            + "\n"
            + "        // Initialize 3 separate file system instances\n"
            + "        AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(uri, config);\n"
            + "        AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(uri, config);\n"
            + "        AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem.newInstance(uri, config);\n"
            + "\n"
            + "        // Create 3 spy clients\n"
            + "        AbfsClient client1 = spyClient(Mockito.spy(fs1), calls1);\n"
            + "        AbfsClient client2 = spyClient(Mockito.spy(fs2), calls2);\n"
            + "        AbfsClient client3 = spyClient(Mockito.spy(fs3), calls3);\n"
            + "\n"
            + "        mgr.registerClient(\"acc1\", client1);\n"
            + "        mgr.registerClient(\"acc2\", client2);\n"
            + "        mgr.registerClient(\"acc3\", client3);\n"
            + "\n"
            + "        // Produce metrics on all clients\n"
            + "        mgr.recordMetric(\"acc1\", \"m1\");\n"
            + "        mgr.recordMetric(\"acc1\", \"m2\");\n"
            + "        mgr.recordMetric(\"acc2\", \"m3\");\n"
            + "        mgr.recordMetric(\"acc2\", \"m4\");\n"
            + "        mgr.recordMetric(\"acc3\", \"m5\");\n"
            + "        mgr.recordMetric(\"acc3\", \"m6\");\n"
            + "\n"
            + "        System.out.println(\"BEFORE_EXIT\");\n"
            + "        crashJvm();\n"
            + "    }\n"
            + "\n"
            + "    private static void crashJvm() throws Exception {\n"
            + "        Field f = sun.misc.Unsafe.class.getDeclaredField(\"theUnsafe\");\n"
            + "        f.setAccessible(true);\n"
            + "        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);\n"
            + "        unsafe.putAddress(0, 0); // SIGSEGV → Immediate JVM crash\n"
            // 128 + 6 = 134 (exitcode for SIGABRT)
            + "    }\n"
            + "\n"
            + "    private static AbfsClient spyClient(AzureBlobFileSystem azureBlobFileSystem, AtomicInteger counter) throws IOException {\n"
            + "        AzureBlobFileSystemStore store = Mockito.spy(azureBlobFileSystem.getAbfsStore());\n"
            + "        Mockito.doReturn(store).when(azureBlobFileSystem).getAbfsStore();\n"
            + "        AbfsClient client = Mockito.spy(store.getClient());\n"
            + "        Mockito.doReturn(client).when(store).getClient();\n"
            + "\n"
            + "        Mockito.doAnswer(inv -> {\n"
            + "          counter.incrementAndGet();\n"
            + "          System.out.println(\"FLUSH:\" + inv.getArguments()[0]);\n"
            + "          return null;\n"
            + "        }).when(client).getMetricCall(Mockito.any());\n"
            + "\n"
            + "        return client;\n"
            + "    }\n"
            + "}\n";

    runProgramAndCaptureOutput(program, false, crashExitCode);
  }

  /**
   * Compiles and runs a Java program in a separate JVM, capturing its output.
   *
   * @param program            The Java program source code as a string.
   * @param expectMetricsFlush Whether to expect metrics flush output.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the thread is interrupted while waiting.
   */
  private void runProgramAndCaptureOutput(String program,
      boolean expectMetricsFlush, int expectedExitCode)
      throws IOException, InterruptedException {
    final long waitTimeInSeconds = 30;
    Path tempFile = Files.createTempFile("ShutdownTestProg", ".java");
    try {
      Files.write(tempFile, program.getBytes(StandardCharsets.UTF_8));

      Path javaFile = tempFile.getParent().resolve("ShutdownTestProg.java");
      Files.move(tempFile, javaFile, StandardCopyOption.REPLACE_EXISTING);

      Process javac = new ProcessBuilder(
          "javac",
          "-classpath", System.getProperty("java.class.path"),
          javaFile.toAbsolutePath().toString())
          .redirectErrorStream(true)
          .start();

      String compileOutput = readProcessOutput(javac);
      javac.waitFor();
      if (!javac.waitFor(waitTimeInSeconds, TimeUnit.SECONDS)) {
        javac.destroyForcibly();
        throw new AssertionError("java process timed out");
      }

      Assertions.assertThat(javac.exitValue())
          .withFailMessage("Compilation failed:\n" + compileOutput)
          .isEqualTo(0);

      String classpath = javaFile.getParent().toAbsolutePath()
          + File.pathSeparator
          + System.getProperty("java.class.path");

      Process javaProc = new ProcessBuilder("java",
          "-XX:ErrorFile=/tmp/no_hs_err_%p.log",
          "-classpath", classpath,
          "ShutdownTestProg")
          .redirectErrorStream(true)
          .start();

      String output = readProcessOutput(javaProc);
      int exitCode;
      if (!javaProc.waitFor(waitTimeInSeconds, TimeUnit.SECONDS)) {
        javaProc.destroyForcibly();
        throw new AssertionError("java process timed out");
      }
      exitCode = javaProc.exitValue();

      Assertions.assertThat(output).contains("BEFORE_EXIT");
      Assertions.assertThat(exitCode).isEqualTo(expectedExitCode);

      if (expectMetricsFlush) {
        Assertions.assertThat(output).contains("FLUSH:");
      } else {
        Assertions.assertThat(output).doesNotContain("FLUSH:");
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  /**
   * Reads all output from a process's input stream.
   *
   * @param proc The process to read from.
   * @return The output as a string.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the thread is interrupted while waiting.
   */
  private static String readProcessOutput(Process proc)
      throws IOException, InterruptedException {
    final int maxBufferSize = 4096;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Thread t = new Thread(() -> {
      try (InputStream in = proc.getInputStream()) {
        byte[] buf = new byte[maxBufferSize];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
    t.start();
    int exitCode = proc.waitFor(); // wait for process to exit
    t.join(); // wait for reading thread to finish
    return out.toString(String.valueOf(StandardCharsets.UTF_8));
  }

  /**
   * Helper method to create a spied client and record timestamp on getMetricCall
   */
  private AbfsClient createSpiedClient(AtomicLong time) throws IOException {
    AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();

    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();

    Mockito.doAnswer(inv -> {
      time.set(System.nanoTime());
      return null;
    }).when(client).getMetricCall(Mockito.any());

    return client;
  }

}
