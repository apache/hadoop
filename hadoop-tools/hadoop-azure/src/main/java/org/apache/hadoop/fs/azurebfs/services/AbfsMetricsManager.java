/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.services.AbfsClient.LOG;

/**
 * AbfsMetricsManager is responsible for managing metrics collection
 * and emission for an AbfsClient instance.
 */
public class AbfsMetricsManager implements Closeable {

  // Timer thread name for AbfsMetricsManager
  public static final String ABFS_CLIENT_TIMER_THREAD_NAME
      = "abfs-timer-client";

  // Timer for scheduling metric emission tasks based on idle time
  private Timer timer;

  // URL for sending metrics
  private URL metricUrl;

  // Shared key credentials for metric account
  private SharedKeyCredentials metricSharedkeyCredentials = null;

  // Currently running TimerTask
  private TimerTask runningTimerTask;

  // Metric analysis periods
  private final int metricAnalysisPeriod;

  // Metric idle period
  private final int metricIdlePeriod;

  // Flag to indicate if a separate metric account is used
  private boolean hasSeparateMetricAccount = false;

  // Flag to indicate if metric collection is enabled
  private final AtomicBoolean isMetricCollectionEnabled
      = new AtomicBoolean(false);

  // Metric format for metrics
  private MetricFormat metricFormat;

  // Flag to indicate if metric collection is stopped
  private final AtomicBoolean isMetricCollectionStopped;

  // AggregateMetricsManager instance
  private final AggregateMetricsManager aggregateMetricsManager;

  // Scheduler to emit aggregated metric based on time
  private ScheduledExecutorService metricsEmitScheduler = null;

  // AbfsConfiguration instance
  private final AbfsConfiguration abfsConfiguration;

  // AbfsCounters instance
  private final AbfsCounters abfsCounters;

  // File system ID
  private final String fileSystemId;

  // Storage account name
  private final String accountName;

  /**
   * Constructor for AbfsMetricsManager.
   *
   * @param abfsConfiguration AbfsConfiguration object.
   * @param abfsCounters      AbfsCounters object.
   * @param baseUrlString     Base URL string of the AbfsClient.
   * @param indexLastForwardSlash Index of last forward slash in the base URL string.
   * @param accountName      Storage account name.
   * @param fileSystemId    File system ID.
   */
  public AbfsMetricsManager(final AbfsConfiguration abfsConfiguration,
      final AbfsCounters abfsCounters, final String baseUrlString,
      final int indexLastForwardSlash, final String accountName,
      final String fileSystemId) {
    this.abfsConfiguration = abfsConfiguration;
    this.abfsCounters = abfsCounters;
    this.fileSystemId = fileSystemId;
    this.isMetricCollectionEnabled.set(
        abfsConfiguration.isMetricsCollectionEnabled());
    this.isMetricCollectionStopped = new AtomicBoolean(false);
    this.aggregateMetricsManager = AggregateMetricsManager.getInstance(
        abfsConfiguration.getMetricsEmitIntervalInMins(),
        abfsConfiguration.getMaxMetricsCallsPerSecond());
    this.metricAnalysisPeriod = abfsConfiguration.getMetricAnalysisTimeout();
    this.metricIdlePeriod = abfsConfiguration.getMetricIdleTimeout();
    this.accountName = accountName;
    if (isMetricCollectionEnabled()) {
      try {
        String metricAccountName = abfsConfiguration.getMetricAccount();
        String metricAccountKey = abfsConfiguration.getMetricAccountKey();
        this.metricFormat = abfsConfiguration.getMetricFormat();
        if (isNotEmpty(metricAccountName) && isNotEmpty(
            metricAccountKey)) {
          int dotIndex = metricAccountName.indexOf(AbfsHttpConstants.DOT);
          if (dotIndex <= 0) {
            throw new InvalidUriException(
                metricAccountName + " - account name is not fully qualified.");
          }
          try {
            metricSharedkeyCredentials = new SharedKeyCredentials(
                metricAccountName.substring(0, dotIndex),
                metricAccountKey);
            hasSeparateMetricAccount = true;
            setMetricsUrl(metricAccountName.startsWith(HTTPS_SCHEME)
                ? metricAccountName : HTTPS_SCHEME + COLON
                + FORWARD_SLASH + FORWARD_SLASH + metricAccountName);
          } catch (IllegalArgumentException e) {
            throw new IOException(
                "Exception while initializing metric credentials ", e);
          }
        } else {
          setMetricsUrl(baseUrlString.substring(0, indexLastForwardSlash + 1));
        }
        // Once the metric URL is set, initialize the metrics
        abfsCounters.initializeMetrics(metricFormat, abfsConfiguration);
        // Metrics emitter scheduler
        this.metricsEmitScheduler
            = Executors.newSingleThreadScheduledExecutor();
        // run every 1 minute to check the metrics count
        this.metricsEmitScheduler.scheduleWithFixedDelay(
            () -> {
              if (abfsCounters.getAbfsBackoffMetrics()
                  .getMetricValue(TOTAL_NUMBER_OF_REQUESTS)
                  >= abfsConfiguration.getMetricsEmitThreshold()) {
                emitCollectedMetrics();
              }
            },
            abfsConfiguration.getMetricsEmitThresholdIntervalInSecs(),
            abfsConfiguration.getMetricsEmitThresholdIntervalInSecs(),
            TimeUnit.SECONDS);

        // run every metricInterval minutes
        this.metricsEmitScheduler.scheduleWithFixedDelay(
            this::emitCollectedMetrics,
            abfsConfiguration.getMetricsEmitIntervalInMins(),
            abfsConfiguration.getMetricsEmitIntervalInMins(),
            TimeUnit.MINUTES);

        // emit metrics based on idea time
        if (abfsConfiguration.shouldEmitMetricsOnIdleTime()) {
          this.timer = new Timer(
              ABFS_CLIENT_TIMER_THREAD_NAME, true);
          timer.schedule(new TimerTaskImpl(),
              metricIdlePeriod,
              metricIdlePeriod);
        }
      } catch (Exception e) {
        LOG.error("Metrics disabled. Failed to initialize metrics for {}",
            baseUrlString, e);
        this.isMetricCollectionEnabled.set(false);
      }
    }
  }

  /**
   * Closes the metrics resources.
   * This method cancels any running timer tasks, shuts down the metrics emission scheduler,
   * and emits any collected metrics before closing.
   */
  @Override
  public void close() {
    if (runningTimerTask != null) {
      runningTimerTask.cancel();
      runningTimerTask = null;
    }
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
    if (metricsEmitScheduler != null && !metricsEmitScheduler.isShutdown()) {
      metricsEmitScheduler.shutdownNow();
      metricsEmitScheduler = null;
    }
    if (isMetricCollectionEnabled()) {
      emitCollectedMetrics();
    }
  }

  /**
   * Retrieves a TracingContext object configured for metric tracking.
   * This method creates a TracingContext object with the validated client correlation ID,
   * the host name of the local machine (or "UnknownHost" if unable to determine),
   * the file system operation type set to GET_ATTR, and additional configuration parameters
   * for metric tracking.
   * The TracingContext is intended for use in tracking metrics related to Azure Blob FileSystem (ABFS) operations.
   *
   * @return A TracingContext object configured for metric tracking.
   */
  private synchronized String getMetricsData() {
    String metrics = abfsCounters.toString();
    if (StringUtils.isEmpty(metrics)) {
      return null;
    }
    abfsCounters.initializeMetrics(metricFormat, abfsConfiguration);
    return TracingContext.validateClientCorrelationID(
        abfsConfiguration.getClientCorrelationId()) + COLON + fileSystemId
        + COLON + metrics;
  }

  /**
   * Synchronized method to suspend or resume timer.
   * @param timerFunctionality resume or suspend.
   * @param timerTask The timertask object.
   * @return true or false.
   */
  boolean timerOrchestrator(TimerFunctionality timerFunctionality,
      TimerTask timerTask) {
    switch (timerFunctionality) {
    case RESUME:
      if (isMetricCollectionEnabled() && isMetricCollectionStopped.get()) {
        synchronized (this) {
          if (isMetricCollectionStopped.get()) {
            resumeTimer();
          }
        }
      }
      break;
    case SUSPEND:
      long now = System.currentTimeMillis();
      long lastExecutionTime = abfsCounters.getLastExecutionTime().get();
      if (isMetricCollectionEnabled() && (now - lastExecutionTime
          >= metricAnalysisPeriod)) {
        synchronized (this) {
          if (!isMetricCollectionStopped.get()) {
            timerTask.cancel();
            timer.purge();
            isMetricCollectionStopped.set(true);
            return true;
          }
        }
      }
      break;
    default:
      break;
    }
    return false;
  }

  /**
   * Resumes the timer for metric collection.
   * This method sets the isMetricCollectionStopped flag to false
   * and schedules a new TimerTaskImpl to run at fixed intervals
   * defined by the metricIdlePeriod.
   */
  private void resumeTimer() {
    isMetricCollectionStopped.set(false);
    timer.schedule(new TimerTaskImpl(),
        metricIdlePeriod,
        metricIdlePeriod);
  }

  /**
   * Checks if metric collection is enabled.
   *
   * @return true if metric collection is enabled, false otherwise.
   */
  public boolean isMetricCollectionEnabled() {
    return isMetricCollectionEnabled.get() && fileSystemId != null;
  }

  /**
   * Getter for metric URL.
   *
   * @return metricUrl
   */
  @VisibleForTesting
  public URL getMetricsUrl() {
    return metricUrl;
  }

  /**
   * Setter for metric URL.
   * Converts blob URL to dfs URL in case of blob storage account.
   *
   * @param urlString to be set as metricUrl.
   * @throws IOException if URL is malformed.
   */
  private void setMetricsUrl(String urlString) throws IOException {
    metricUrl = UriUtils.changeUrlFromBlobToDfs(new URL(urlString));
  }

  /**
   * TimerTask implementation for emitting collected metrics based on ideal time.
   * This class extends TimerTask and overrides the run method to
   * check if the timer should be suspended based on the configured
   * metric analysis period. If the timer is suspended, it triggers
   * the emission of collected metrics.
   */
  class TimerTaskImpl extends TimerTask {

    TimerTaskImpl() {
      runningTimerTask = this;
    }

    @Override
    public void run() {
      if (timerOrchestrator(TimerFunctionality.SUSPEND, this)) {
        emitCollectedMetrics();
      }
    }
  }

  /**
   * Emits the collected metrics by making a metric call to the Azure Blob FileSystem (ABFS).
   * This method checks if metric collection is enabled and, if so, attempts to perform
   * a metric call using the configured tracing context. Any IOException encountered during
   * the metric call is logged and ignored to prevent termination of the timer task.
   * Finally, it re-initializes the metrics in the AbfsCounters instance using the specified
   * metric format.
   */
  public void emitCollectedMetrics() {
    if (!isMetricCollectionEnabled()) {
      return;
    }
    this.aggregateMetricsManager.recordMetric(accountName, getMetricsData());
  }

  /**
   * Getter for timer.
   */
  @VisibleForTesting
  protected Timer getTimer() {
    return timer;
  }

  /**
   * Getter for metricsEmitScheduler.
   */
  @VisibleForTesting
  ScheduledExecutorService getMetricsEmitScheduler() {
    return metricsEmitScheduler;
  }

  /**
   * @return true if metric account name and key are different from storage account.
   */
  public boolean hasSeparateMetricAccount() {
    return hasSeparateMetricAccount;
  }

  /**
   * Getter for metric shared key credentials.
   */
  public SharedKeyCredentials getMetricSharedkeyCredentials() {
    return metricSharedkeyCredentials;
  }

  /**
   * Getter for AggregateMetricsManager.
   */
  public AggregateMetricsManager getAggregateMetricsManager() {
    return aggregateMetricsManager;
  }
}
