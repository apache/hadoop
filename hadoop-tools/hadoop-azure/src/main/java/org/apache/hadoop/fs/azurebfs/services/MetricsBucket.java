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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.utils.SimpleRateLimiter;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CLOSING_SQUARE_BRACKET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OPENING_SQUARE_BRACKET;

/**
 * MetricsBucket holds metrics for multiple AbfsClients and
 * dispatches them in batches, respecting rate limits.
 */
final class MetricsBucket {

  // Logger for the class.
  private static final Logger LOG = LoggerFactory.getLogger(MetricsBucket.class);

  // Rate limiter to control the rate of dispatching metrics.
  private final SimpleRateLimiter rateLimiter;

  // Buffer to hold metrics before sending.
  private final AtomicReference<ConcurrentLinkedQueue<String>> metricsBuffer =
      new AtomicReference<>(new ConcurrentLinkedQueue<>());

  // Set of registered AbfsClients.
  private final Set<AbfsClient> clients =
      ConcurrentHashMap.newKeySet();

  // Maximum size of metrics header in characters.
  private static final long MAX_HEADER_SIZE = 1024;

  // Constructor
  MetricsBucket(SimpleRateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
  }

  /**
   * Register a new AbfsClient.
   * @param client the AbfsClient to register
   */
  public void registerClient(AbfsClient client) {
    if (client != null) {
      clients.add(client);
    }
  }

  /**
   * Deregister an AbfsClient. If this is the last client, drain and send
   * any remaining metrics.
   * @param client the AbfsClient to deregister
   * @return true if the client was deregistered, false otherwise
   */
  public boolean deregisterClient(AbfsClient client) {
    if (client == null) {
      return false;
    }
    ConcurrentLinkedQueue<String> batchToSend = null;
    boolean isLastClient = false;

    synchronized (this) {
      if (!clients.contains(client)) {
        return false;
      }

      if (clients.size() == 1) {
        // This client is the last one — drain metrics now
        batchToSend = metricsBuffer.getAndSet(new ConcurrentLinkedQueue<>());
        isLastClient = true;
      }

      clients.remove(client);
    }
    if (isLastClient) {
      sendMetrics(client, batchToSend);
    }
    return true;
  }

  /**
   * Add a metric to the buffer.
   * @param metric the metric to add
   */
  void addRequest(String metric) {
    if (metric != null) {
      metricsBuffer.get().add(metric);
    }
  }

  /**
   * Drain the metrics buffer and send if there are registered clients.
   */
  public void drainAndSendIfReady() {
    AbfsClient client;

    synchronized (this) {
      if (clients.isEmpty()) {
        return;
      }
      client = clients.iterator().next();
    }

    ConcurrentLinkedQueue<String> batch = metricsBuffer.getAndSet(
        new ConcurrentLinkedQueue<>());
    if (batch.isEmpty()) {
      return;
    }

    sendMetrics(client, batch);
  }

  // Send metrics outside synchronized block
  private void sendMetrics(AbfsClient client,
      ConcurrentLinkedQueue<String> batchToSend) {
    // Send outside synchronized block
    if (client != null && batchToSend != null && !batchToSend.isEmpty()) {
      for (String chunk : splitListBySize(batchToSend, MAX_HEADER_SIZE)) {
        rateLimiter.acquire(5, TimeUnit.SECONDS); // Rate limiting
        try {
          client.getMetricCall(chunk);
        } catch (IOException ignored) {
          LOG.debug("Failed to send metrics: {}", ignored.getMessage());
        }
      }
    }
  }

  // Check if there are no registered clients
  public synchronized boolean isEmpty() {
    return clients.isEmpty();
  }

  /**
   * Split the list of metrics into chunks that fit within maxChars.
   * Each metric is wrapped in square brackets and separated by colons.
   */
  private static List<String> splitListBySize(
      ConcurrentLinkedQueue<String> items, long maxChars) {

    if (items.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> result = new ArrayList<>();
    StringBuilder sb = new StringBuilder();

    for (String s : items) {
      String wrapped = OPENING_SQUARE_BRACKET + s + CLOSING_SQUARE_BRACKET;
      int additional =
          sb.length() == 0 ? wrapped.length()
              : wrapped.length() + 1;

      if (wrapped.length() > maxChars) {
        if (sb.length() > 0) {
          result.add(sb.toString());
          sb.setLength(0);
        }
        result.add(wrapped);
        continue;
      }

      if (sb.length() + additional > maxChars) {
        result.add(sb.toString());
        sb.setLength(0);
      }

      if (sb.length() > 0) {
        sb.append(COLON);
      }
      sb.append(wrapped);
    }

    if (sb.length() > 0) {
      result.add(sb.toString());
    }

    return result;
  }
}
