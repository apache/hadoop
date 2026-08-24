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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.Test;

/**
 * HttpServer2Metrics publishes what it published on Jetty 9.4: the same
 * metric names, and times in milliseconds.
 */
public class TestHttpServer2Metrics {

  @Test
  public void testTimesArePublishedInMillis() {
    long max = TimeUnit.MILLISECONDS.toNanos(1500);
    long total = TimeUnit.MILLISECONDS.toNanos(4000);
    double mean = TimeUnit.MILLISECONDS.toNanos(250);
    double stdDev = TimeUnit.MICROSECONDS.toNanos(500);

    StatisticsHandler handler = mock(StatisticsHandler.class);
    when(handler.getRequestTimeMax()).thenReturn(max);
    when(handler.getRequestTimeTotal()).thenReturn(total);
    when(handler.getRequestTimeMean()).thenReturn(mean);
    when(handler.getRequestTimeStdDev()).thenReturn(stdDev);
    when(handler.getHandleTimeMax()).thenReturn(max);
    when(handler.getHandleTimeTotal()).thenReturn(total);
    when(handler.getHandleTimeMean()).thenReturn(mean);
    when(handler.getHandleTimeStdDev()).thenReturn(stdDev);

    HttpServer2Metrics metrics = new HttpServer2Metrics(handler, 0,
        mock(QueuedThreadPool.class), 1, 1);

    assertEquals(1500, metrics.requestTimeMax());
    assertEquals(4000, metrics.requestTimeTotal());
    assertEquals(250.0, metrics.requestTimeMean(), 1e-9);
    assertEquals(0.5, metrics.requestTimeStdDev(), 1e-9);
    assertEquals(1500, metrics.dispatchedTimeMax());
    assertEquals(4000, metrics.dispatchedTimeTotal());
    assertEquals(250.0, metrics.dispatchedTimeMean(), 1e-9);
    assertEquals(0.5, metrics.dispatchedTimeStdDev(), 1e-9);
  }

  /**
   * Jetty 12 has nothing to source the async metrics from. They are still
   * published, so the names monitoring reads do not vanish, and read 0: what
   * they read on 9.4 for every servlet Hadoop serves.
   */
  @Test
  public void testAsyncMetricsAreStillPublished() {
    HttpServer2Metrics metrics = new HttpServer2Metrics(
        mock(StatisticsHandler.class), 0, mock(QueuedThreadPool.class), 1, 1);
    assertEquals(0, metrics.asyncDispatches());
    assertEquals(0, metrics.asyncRequests());
    assertEquals(0, metrics.asyncRequestsWaiting());
    assertEquals(0, metrics.asyncRequestsWaitingMax());
    assertEquals(0, metrics.expires());
  }
}
