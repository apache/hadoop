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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.eclipse.jetty.server.handler.StatisticsHandler;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(name="HttpServer2", about="HttpServer2 metrics", context="http")
public class HttpServer2Metrics {

  private final StatisticsHandler handler;

  @Metric public int asyncDispatches() {
    return handler.getAsyncDispatches();
  }
  @Metric public int asyncRequests() {
    return handler.getAsyncRequests();
  }
  @Metric public int asyncRequestsWaiting() {
    return handler.getAsyncRequestsWaiting();
  }
  @Metric public int asyncRequestsWaitingMax() {
    return handler.getAsyncRequestsWaitingMax();
  }
  @Metric public int dispatched() {
    return handler.getDispatched();
  }
  @Metric public int dispatchedActive() {
    return handler.getDispatchedActive();
  }
  @Metric public int dispatchedActiveMax() {
    return handler.getDispatchedActiveMax();
  }
  @Metric public long dispatchedTimeMax() {
    return handler.getDispatchedTimeMax();
  }
  @Metric public double dispatchedTimeMean() {
    return handler.getDispatchedTimeMean();
  }
  @Metric public double dispatchedTimeStdDev() {
    return handler.getDispatchedTimeStdDev();
  }
  @Metric public long dispatchedTimeTotal() {
    return handler.getDispatchedTimeTotal();
  }
  @Metric public int expires() {
    return handler.getExpires();
  }
  @Metric public int requests() {
    return handler.getRequests();
  }
  @Metric public int requestsActive() {
    return handler.getRequestsActive();
  }
  @Metric public int requestsActiveMax() {
    return handler.getRequestsActiveMax();
  }
  @Metric public long requestTimeMax() {
    return handler.getRequestTimeMax();
  }
  @Metric public double requestTimeMean() {
    return handler.getRequestTimeMean();
  }
  @Metric public double requestTimeStdDev() {
    return handler.getRequestTimeStdDev();
  }
  @Metric public long requestTimeTotal() {
    return handler.getRequestTimeTotal();
  }
  @Metric public int responses1xx() {
    return handler.getResponses1xx();
  }
  @Metric public int responses2xx() {
    return handler.getResponses2xx();
  }
  @Metric public int responses3xx() {
    return handler.getResponses3xx();
  }
  @Metric public int responses4xx() {
    return handler.getResponses4xx();
  }
  @Metric public int responses5xx() {
    return handler.getResponses5xx();
  }
  @Metric public long getResponseBytesTotal() {
    return handler.getResponsesBytesTotal();
  }
  @Metric public long statsOnMs() {
    return handler.getStatsOnMs();
  }

  HttpServer2Metrics(StatisticsHandler handler) {
    this.handler = handler;
  }

  public static HttpServer2Metrics create(StatisticsHandler handler) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(new HttpServer2Metrics(handler));
  }
}
