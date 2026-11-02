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
package org.apache.hadoop.http;

import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the thread-pool metrics exposed by {@link HttpServer2Metrics}.
 */
public class TestHttpServer2Metrics {

  private static final int MIN_THREADS = 3;
  private static final int MAX_THREADS = 17;
  private static final int PORT = 0;

  private QueuedThreadPool threadPool;

  @BeforeEach
  public void setUp() throws Exception {
    threadPool = new QueuedThreadPool(MAX_THREADS, MIN_THREADS);
    threadPool.setName("test-http-threadpool");
    threadPool.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (threadPool != null) {
      threadPool.stop();
    }
  }

  @Test
  public void testThreadPoolMetrics() {
    final StatisticsHandler handler = new StatisticsHandler();
    final HttpServer2Metrics metrics =
        new HttpServer2Metrics(handler, threadPool, PORT);

    // Configured bounds are exposed verbatim.
    assertEquals(MAX_THREADS, metrics.httpThreadsMax(),
        "max threads should match the thread pool configuration");
    assertEquals(MIN_THREADS, metrics.httpThreadsMin(),
        "min threads should match the thread pool configuration");

    // Live gauges must be internally consistent and non-negative.
    final int threads = metrics.httpThreads();
    final int idle = metrics.httpThreadsIdle();
    final int busy = metrics.httpThreadsBusy();
    final int queueSize = metrics.httpThreadsQueueSize();
    final int lowOnThreads = metrics.httpThreadsLowOnThreads();

    assertTrue(threads >= 0, "live thread count should be non-negative");
    assertTrue(idle >= 0, "idle thread count should be non-negative");
    assertTrue(busy >= 0, "busy thread count should be non-negative");
    assertTrue(queueSize >= 0, "queue size should be non-negative");
    assertTrue(lowOnThreads == 0 || lowOnThreads == 1,
        "lowOnThreads must be expressed as 0 or 1");
    assertTrue(threads <= MAX_THREADS,
        "live thread count should not exceed the configured maximum");
  }

  @Test
  public void testCreateRegistersThreadPoolSource() {
    final StatisticsHandler handler = new StatisticsHandler();
    final HttpServer2Metrics metrics =
        HttpServer2Metrics.create(handler, threadPool, PORT);
    try {
      assertEquals(MAX_THREADS, metrics.httpThreadsMax(),
          "registered source should expose the thread pool max threads");
    } finally {
      metrics.remove();
    }
  }
}
