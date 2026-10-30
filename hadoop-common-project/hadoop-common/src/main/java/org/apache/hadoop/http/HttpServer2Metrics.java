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

import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * This class collects all the metrics of Jetty's StatisticsHandler
 * and expose them as Hadoop Metrics.
 *
 * Jetty 12 rebuilt StatisticsHandler around the core request lifecycle. The
 * dispatch counters it used to publish are now handle counters measuring the
 * same thing under a different name, and are read as such here so the metric
 * names Hadoop emits do not move. Its four async counters and its count of
 * expired async requests have no counterpart, because the core no longer sees
 * servlet async activity. Those five are still published, as 0, so that the
 * metric names do not disappear from JMX and from the sinks that monitoring
 * reads them through. That is a change in value: on Jetty 9.4 they were not
 * always 0, because its DefaultServlet sent static files - the web UI's
 * pages, scripts and stylesheets - asynchronously, and each of those counted
 * as an async request. No Hadoop servlet starts one itself.
 *
 * Jetty 12 also records every time statistic in nanoseconds, where 9.4
 * recorded milliseconds. The metrics below are documented, and have always
 * been published, in milliseconds, so they are converted on the way out.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(name="HttpServer2", about="HttpServer2 metrics", context="http")
public class HttpServer2Metrics {

  private final StatisticsHandler handler;
  private final int port;
  private final QueuedThreadPool threadPool;
  private final int acceptorThreads;
  private final int selectorThreads;

  @Metric("number of requested that have been asynchronously dispatched;"
      + " always 0 since Jetty 12")
  public int asyncDispatches() {
    return 0;
  }
  @Metric("total number of async requests; always 0 since Jetty 12")
  public int asyncRequests() {
    return 0;
  }
  @Metric("currently waiting async requests; always 0 since Jetty 12")
  public int asyncRequestsWaiting() {
    return 0;
  }
  @Metric("maximum number of waiting async requests; always 0 since Jetty 12")
  public int asyncRequestsWaitingMax() {
    return 0;
  }
  @Metric("number of dispatches")
  public int dispatched() {
    return handler.getHandleTotal();
  }
  @Metric("number of dispatches currently active")
  public int dispatchedActive() {
    return handler.getHandleActive();
  }
  @Metric("maximum number of active dispatches being handled")
  public int dispatchedActiveMax() {
    return handler.getHandleActiveMax();
  }
  @Metric("maximum time spend in dispatch handling (in ms)")
  public long dispatchedTimeMax() {
    return nanosToMillis(handler.getHandleTimeMax());
  }
  @Metric("mean time spent in dispatch handling (in ms)")
  public double dispatchedTimeMean() {
    return nanosToMillis(handler.getHandleTimeMean());
  }
  @Metric("standard deviation for dispatch handling (in ms)")
  public double dispatchedTimeStdDev() {
    return nanosToMillis(handler.getHandleTimeStdDev());
  }
  @Metric("total time spent in dispatch handling (in ms)")
  public long dispatchedTimeTotal() {
    return nanosToMillis(handler.getHandleTimeTotal());
  }
  @Metric("number of async requests requests that have expired;"
      + " always 0 since Jetty 12")
  public int expires() {
    return 0;
  }
  @Metric("number of requests")
  public int requests() {
    return handler.getRequests();
  }
  @Metric("number of requests currently active")
  public int requestsActive() {
    return handler.getRequestsActive();
  }
  @Metric("maximum number of active requests")
  public int requestsActiveMax() {
    return handler.getRequestsActiveMax();
  }
  @Metric("maximum time spend handling requests (in ms)")
  public long requestTimeMax() {
    return nanosToMillis(handler.getRequestTimeMax());
  }
  @Metric("mean time spent handling requests (in ms)")
  public double requestTimeMean() {
    return nanosToMillis(handler.getRequestTimeMean());
  }
  @Metric("standard deviation for request handling (in ms)")
  public double requestTimeStdDev() {
    return nanosToMillis(handler.getRequestTimeStdDev());
  }
  @Metric("total time spend in all request handling (in ms)")
  public long requestTimeTotal() {
    return nanosToMillis(handler.getRequestTimeTotal());
  }
  @Metric("number of requests with 1xx response status")
  public int responses1xx() {
    return handler.getResponses1xx();
  }
  @Metric("number of requests with 2xx response status")
  public int responses2xx() {
    return handler.getResponses2xx();
  }
  @Metric("number of requests with 3xx response status")
  public int responses3xx() {
    return handler.getResponses3xx();
  }
  @Metric("number of requests with 4xx response status")
  public int responses4xx() {
    return handler.getResponses4xx();
  }
  @Metric("number of requests with 5xx response status")
  public int responses5xx() {
    return handler.getResponses5xx();
  }
  @Metric("total number of bytes across all responses")
  public long responsesBytesTotal() {
    return handler.getBytesWritten();
  }
  @Metric("time in milliseconds stats have been collected for")
  public long statsOnMs() {
    return handler.getStatisticsDuration().toMillis();
  }
  @Metric("maximum number of threads in the pool")
  public int maxThreads() {
    return threadPool.getMaxThreads();
  }
  @Metric("number of idle threads in the pool")
  public int idleThreads() {
    return threadPool.getIdleThreads();
  }
  @Metric("number of busy threads in the pool")
  public int busyThreads() {
    return threadPool.getBusyThreads();
  }
  @Metric("number of threads in the pool")
  public int threads() {
    return threadPool.getThreads();
  }
  @Metric("minimum number of threads in the pool")
  public int minThreads() {
    return threadPool.getMinThreads();
  }
  @Metric("size of the job queue")
  public int queueSize() {
    return threadPool.getQueueSize();
  }
  @Metric("number of acceptor threads across all connectors")
  public int acceptorThreads() {
    return acceptorThreads;
  }
  @Metric("number of selector threads across all connectors")
  public int selectorThreads() {
    return selectorThreads;
  }
  @Metric("maximum number of worker threads in the pool")
  public int maxWorkerThreads() {
    return threadPool.getMaxThreads() - acceptorThreads - selectorThreads;
  }
  @Metric("number of busy worker threads in the pool")
  public int busyWorkerThreads() {
    return threadPool.getBusyThreads() - acceptorThreads - selectorThreads;
  }
  @Metric("number of worker threads in the pool")
  public int workerThreads() {
    return threadPool.getThreads() - acceptorThreads - selectorThreads;
  }

  HttpServer2Metrics(StatisticsHandler handler, int port,
      QueuedThreadPool threadPool, int acceptorThreads, int selectorThreads) {
    this.handler = handler;
    this.port = port;
    this.threadPool = threadPool;
    this.acceptorThreads = acceptorThreads;
    this.selectorThreads = selectorThreads;
  }

  static HttpServer2Metrics create(StatisticsHandler handler, int port,
      QueuedThreadPool threadPool, int acceptorThreads, int selectorThreads) {
    final MetricsSystem ms = DefaultMetricsSystem.instance();
    final HttpServer2Metrics metrics = new HttpServer2Metrics(handler, port,
        threadPool, acceptorThreads, selectorThreads);
    // Remove the old metrics from metrics system to avoid duplicate error
    // when HttpServer2 is started twice.
    metrics.remove();
    // Add port number to the suffix to allow multiple instances in a host.
    return ms.register("HttpServer2-" + port, "HttpServer2 metrics", metrics);
  }

  void remove() {
    DefaultMetricsSystem.removeSourceName("HttpServer2-" + port);
  }

  private static long nanosToMillis(long nanos) {
    return TimeUnit.NANOSECONDS.toMillis(nanos);
  }

  private static double nanosToMillis(double nanos) {
    return nanos / TimeUnit.MILLISECONDS.toNanos(1);
  }
}
