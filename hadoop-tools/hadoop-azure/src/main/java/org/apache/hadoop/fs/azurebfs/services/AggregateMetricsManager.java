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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.utils.SimpleRateLimiter;

/**
 * AggregateMetricsManager manages metrics collection and dispatching
 * for multiple AbfsClients across different accounts.
 */
public final class AggregateMetricsManager {

  // Singleton instance of AggregateMetricsManager.
  private static volatile AggregateMetricsManager instance;

  // Rate limiter to control the rate of dispatching metrics.
  private static volatile SimpleRateLimiter rateLimiter;

  // Map of account name to MetricsBucket.
  private final ConcurrentHashMap<String, MetricsBucket> buckets =
      new ConcurrentHashMap<>();

  // Scheduler for periodic dispatching of metrics.
  private final ScheduledExecutorService scheduler;

  // Private constructor to enforce singleton pattern.
  private AggregateMetricsManager(final long dispatchIntervalInMins,
      final int permitsPerSecond) throws InvalidConfigurationValueException {

    if (dispatchIntervalInMins <= 0) {
      throw new InvalidConfigurationValueException(
          "dispatchIntervalInMins must be > 0");
    }

    if (permitsPerSecond <= 0) {
      throw new InvalidConfigurationValueException(
          "permitsPerSecond must be > 0");
    }

    rateLimiter = new SimpleRateLimiter(permitsPerSecond);

    // Initialize scheduler for periodic dispatching of metrics.
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "ABFS-Aggregated-Metrics-Dispatcher");
      t.setDaemon(true);
      return t;
    });

    // Schedule periodic dispatching of metrics.
    this.scheduler.scheduleWithFixedDelay(
        this::dispatchMetrics,
        dispatchIntervalInMins,
        dispatchIntervalInMins,
        TimeUnit.MINUTES);

    // Add shutdown hook to dispatch remaining metrics on JVM shutdown.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      dispatchMetrics();
      scheduler.shutdown();
    }));
  }

  /**
   * Get the singleton instance of AggregateMetricsManager.
   *
   * @param dispatchIntervalInMins Interval in minutes for dispatching metrics.
   * @param permitsPerSecond       Rate limit for dispatching metrics.
   * @return Singleton instance of AggregateMetricsManager.
   */
  public static AggregateMetricsManager getInstance(final long dispatchIntervalInMins,
      final int permitsPerSecond) {
    if (instance != null) {
      return instance;
    }

    synchronized (AggregateMetricsManager.class) {
      if (instance == null) {
        try {
          instance = new AggregateMetricsManager(
              dispatchIntervalInMins, permitsPerSecond);
        } catch (InvalidConfigurationValueException e) {
          throw new RuntimeException(
              "Failed to initialize AggregateMetricsManager", e);
        }
      }
      return instance;
    }
  }

  /**
   * Register an AbfsClient with the manager.
   * @param account Account name.
   * @param abfsClient AbfsClient instance.
   */
  public void registerClient(String account, AbfsClient abfsClient) {
    if (StringUtils.isEmpty(account) || abfsClient == null) {
      return;
    }

    buckets.computeIfAbsent(account,
            key -> new MetricsBucket(rateLimiter))
        .registerClient(abfsClient);
  }

  /**
   * Deregister an AbfsClient from the manager.
   * @param account Account name.
   * @param abfsClient AbfsClient instance.
   * @return true if the client was deregistered, false otherwise.
   */
  public boolean deregisterClient(String account, AbfsClient abfsClient) {
    if (StringUtils.isEmpty(account) || abfsClient == null) {
      return false;
    }

    AtomicBoolean isRemoved = new AtomicBoolean(false);

    buckets.computeIfPresent(account, (key, bucket) -> {
      // Deregister the client
      isRemoved.set(bucket.deregisterClient(abfsClient));

      // If bucket became empty, remove it atomically
      return bucket.isEmpty() ? null : bucket;
    });

    return isRemoved.get();
  }

  /**
   * Record metrics data for a specific account.
   * @param accountName Account name.
   * @param metricsData Metrics data to record.
   */
  public void recordMetric(String accountName, String metricsData) {
    if (StringUtils.isEmpty(accountName)
        || StringUtils.isEmpty(metricsData)) {
      return;
    }

    MetricsBucket bucket = buckets.get(accountName);
    if (bucket == null) {
      return;
    }

    bucket.addRequest(metricsData);
  }

  // Dispatch metrics for all buckets.
  private void dispatchMetrics() {
    buckets.values().forEach(MetricsBucket::drainAndSendIfReady);
  }
}
