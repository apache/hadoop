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

package org.apache.hadoop.fs.azurebfs;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.services.AbfsBackoffMetrics;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsReadFooterMetrics;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;
import org.apache.hadoop.metrics2.MetricStringBuilder;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.BYTES_RECEIVED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.BYTES_SENT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_APPEND;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_CREATE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_CREATE_NON_RECURSIVE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_DELETE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_EXIST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_GET_DELEGATION_TOKEN;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_GET_FILE_STATUS;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_LIST_STATUS;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_MKDIRS;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_OPEN;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_RENAME;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.DIRECTORIES_CREATED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.DIRECTORIES_DELETED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.ERROR_IGNORED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.FILES_CREATED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.FILES_DELETED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.GET_RESPONSES;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_DELETE_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_HEAD_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_PATCH_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_POST_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_PUT_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.METADATA_INCOMPLETE_RENAME_FAILURES;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.READ_THROTTLES;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_RECOVERY;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.SEND_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.SERVER_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.WRITE_THROTTLES;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.util.Time.now;


/**
 * Instrumentation of Abfs counters.
 */
public class AbfsCountersImpl implements AbfsCounters {

  /**
   * Single context for all the Abfs counters to separate them from other
   * counters.
   */
  private static final String CONTEXT = "AbfsContext";
  /**
   * The name of a field added to metrics records that uniquely identifies a
   * specific FileSystem instance.
   */
  private static final String REGISTRY_ID = "AbfsID";
  /**
   * The name of a field added to metrics records that indicates the hostname
   * portion of the FS URL.
   */
  private static final String METRIC_BUCKET = "AbfsBucket";

  private final MetricsRegistry registry =
      new MetricsRegistry("abfsMetrics").setContext(CONTEXT);

  private final IOStatisticsStore ioStatisticsStore;

  private AbfsBackoffMetrics abfsBackoffMetrics = null;

  private AbfsReadFooterMetrics abfsReadFooterMetrics = null;

  private AtomicLong lastExecutionTime = null;

  private static final AbfsStatistic[] STATISTIC_LIST = {
      CALL_CREATE,
      CALL_OPEN,
      CALL_GET_FILE_STATUS,
      CALL_APPEND,
      CALL_CREATE_NON_RECURSIVE,
      CALL_DELETE,
      CALL_EXIST,
      CALL_GET_DELEGATION_TOKEN,
      CALL_LIST_STATUS,
      CALL_MKDIRS,
      CALL_RENAME,
      DIRECTORIES_CREATED,
      DIRECTORIES_DELETED,
      FILES_CREATED,
      FILES_DELETED,
      ERROR_IGNORED,
      CONNECTIONS_MADE,
      SEND_REQUESTS,
      GET_RESPONSES,
      BYTES_SENT,
      BYTES_RECEIVED,
      READ_THROTTLES,
      WRITE_THROTTLES,
      SERVER_UNAVAILABLE,
      RENAME_RECOVERY,
      METADATA_INCOMPLETE_RENAME_FAILURES,
      RENAME_PATH_ATTEMPTS
  };

  private static final AbfsStatistic[] DURATION_TRACKER_LIST = {
      HTTP_HEAD_REQUEST,
      HTTP_GET_REQUEST,
      HTTP_DELETE_REQUEST,
      HTTP_PUT_REQUEST,
      HTTP_PATCH_REQUEST,
      HTTP_POST_REQUEST
  };

  public AbfsCountersImpl(URI uri) {
    UUID fileSystemInstanceId = UUID.randomUUID();
    registry.tag(REGISTRY_ID,
        "A unique identifier for the instance",
        fileSystemInstanceId.toString());
    registry.tag(METRIC_BUCKET, "Hostname from the FS URL", uri.getHost());

    IOStatisticsStoreBuilder ioStatisticsStoreBuilder = iostatisticsStore();
    // Declaring the counters.
    for (AbfsStatistic stats : STATISTIC_LIST) {
      ioStatisticsStoreBuilder.withCounters(stats.getStatName());
      createCounter(stats);
    }
    // Declaring the DurationTrackers.
    for (AbfsStatistic durationStats : DURATION_TRACKER_LIST) {
      ioStatisticsStoreBuilder.withDurationTracking(durationStats.getStatName());
    }
    ioStatisticsStore = ioStatisticsStoreBuilder.build();
    lastExecutionTime = new AtomicLong(now());
  }

  @Override
  public void initializeMetrics(MetricFormat metricFormat) {
    switch (metricFormat) {
      case INTERNAL_BACKOFF_METRIC_FORMAT:
        abfsBackoffMetrics = new AbfsBackoffMetrics();
        break;
      case INTERNAL_FOOTER_METRIC_FORMAT:
        abfsReadFooterMetrics = new AbfsReadFooterMetrics();
        break;
      case INTERNAL_METRIC_FORMAT:
        abfsBackoffMetrics = new AbfsBackoffMetrics();
        abfsReadFooterMetrics = new AbfsReadFooterMetrics();
        break;
      default:
        break;
    }
  }

  /**
   * Look up a Metric from registered set.
   *
   * @param name name of metric.
   * @return the metric or null.
   */
  private MutableMetric lookupMetric(String name) {
    return getRegistry().get(name);
  }

  /**
   * Look up counter by name.
   *
   * @param name name of counter.
   * @return counter if found, else null.
   */
  private MutableCounterLong lookupCounter(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      return null;
    }
    if (!(metric instanceof MutableCounterLong)) {
      throw new IllegalStateException("Metric " + name
          + " is not a MutableCounterLong: " + metric);
    }
    return (MutableCounterLong) metric;
  }

  /**
   * Create a counter in the registry.
   *
   * @param stats AbfsStatistic whose counter needs to be made.
   * @return counter or null.
   */
  private MutableCounterLong createCounter(AbfsStatistic stats) {
    return registry.newCounter(stats.getStatName(),
        stats.getStatDescription(), 0L);
  }

  /**
   * {@inheritDoc}
   *
   * Increment a statistic with some value.
   *
   * @param statistic AbfsStatistic need to be incremented.
   * @param value     long value to be incremented by.
   */
  @Override
  public void incrementCounter(AbfsStatistic statistic, long value) {
    ioStatisticsStore.incrementCounter(statistic.getStatName(), value);
    MutableCounterLong counter = lookupCounter(statistic.getStatName());
    if (counter != null) {
      counter.incr(value);
    }
  }

  /**
   * Getter for MetricRegistry.
   *
   * @return MetricRegistry or null.
   */
  private MetricsRegistry getRegistry() {
    return registry;
  }

  @Override
  public AbfsBackoffMetrics getAbfsBackoffMetrics() {
    return abfsBackoffMetrics != null ? abfsBackoffMetrics : null;
  }

  @Override
  public AtomicLong getLastExecutionTime() {
    return lastExecutionTime;
  }

  @Override
  public AbfsReadFooterMetrics getAbfsReadFooterMetrics() {
    return abfsReadFooterMetrics != null ? abfsReadFooterMetrics : null;
  }

  /**
   * {@inheritDoc}
   *
   * Method to aggregate all the counters in the MetricRegistry and form a
   * string with prefix, separator and suffix.
   *
   * @param prefix    string that would be before metric.
   * @param separator string that would be between metric name and value.
   * @param suffix    string that would be after metric value.
   * @param all       gets all the values even if unchanged.
   * @return a String with all the metrics and their values.
   */
  @Override
  public String formString(String prefix, String separator, String suffix,
      boolean all) {

    MetricStringBuilder metricStringBuilder = new MetricStringBuilder(null,
        prefix, separator, suffix);
    registry.snapshot(metricStringBuilder, all);
    return metricStringBuilder.toString();
  }

  /**
   * {@inheritDoc}
   *
   * Map of all the counters for testing.
   *
   * @return a map of the IOStatistics counters.
   */
  @VisibleForTesting
  @Override
  public Map<String, Long> toMap() {
    return ioStatisticsStore.counters();
  }

  /**
   * Returning the instance of IOStatisticsStore used to collect the metrics
   * in AbfsCounters.
   *
   * @return instance of IOStatistics.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return ioStatisticsStore;
  }

  /**
   * Tracks the duration of a statistic.
   *
   * @param key name of the statistic.
   * @return DurationTracker for that statistic.
   */
  @Override
  public DurationTracker trackDuration(String key) {
    return ioStatisticsStore.trackDuration(key);
  }

  @Override
  public String toString() {
    String metric = "";
    if (abfsBackoffMetrics != null) {
      long totalNoRequests = getAbfsBackoffMetrics().getMetricValue(TOTAL_NUMBER_OF_REQUESTS);
      if (totalNoRequests > 0) {
        metric += "#BO:" + getAbfsBackoffMetrics().toString();
      }
    }
    if (abfsReadFooterMetrics != null) {
      if (getAbfsReadFooterMetrics().getTotalFiles() > 0) {
        metric += "#FO:" + getAbfsReadFooterMetrics().toString();
      }
    }
    return metric;
  }
}
