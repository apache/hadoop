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

package org.apache.hadoop.fs.azure.metrics;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * A metrics source for the WASB file system to track all the metrics we care
 * about for getting a clear picture of the performance/reliability/interaction
 * of the Hadoop cluster with Azure Storage.
 */
@Metrics(about="Metrics for WASB", context="azureFileSystem")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AzureFileSystemInstrumentation implements MetricsSource {

  public static final String METRIC_TAG_FILESYSTEM_ID = "wasbFileSystemId";
  public static final String METRIC_TAG_ACCOUNT_NAME = "accountName";
  public static final String METRIC_TAG_CONTAINTER_NAME = "containerName";

  public static final String WASB_WEB_RESPONSES = "wasb_web_responses";
  public static final String WASB_BYTES_WRITTEN =
      "wasb_bytes_written_last_second";
  public static final String WASB_BYTES_READ =
      "wasb_bytes_read_last_second";
  public static final String WASB_RAW_BYTES_UPLOADED =
      "wasb_raw_bytes_uploaded";
  public static final String WASB_RAW_BYTES_DOWNLOADED =
      "wasb_raw_bytes_downloaded";
  public static final String WASB_FILES_CREATED = "wasb_files_created";
  public static final String WASB_FILES_DELETED = "wasb_files_deleted";
  public static final String WASB_DIRECTORIES_CREATED = "wasb_directories_created";
  public static final String WASB_DIRECTORIES_DELETED = "wasb_directories_deleted";
  public static final String WASB_UPLOAD_RATE =
      "wasb_maximum_upload_bytes_per_second";
  public static final String WASB_DOWNLOAD_RATE =
      "wasb_maximum_download_bytes_per_second";
  public static final String WASB_UPLOAD_LATENCY =
      "wasb_average_block_upload_latency_ms";
  public static final String WASB_DOWNLOAD_LATENCY =
      "wasb_average_block_download_latency_ms";
  public static final String WASB_CLIENT_ERRORS = "wasb_client_errors";
  public static final String WASB_SERVER_ERRORS = "wasb_server_errors";

  /**
   * Config key for how big the rolling window size for latency metrics should
   * be (in seconds).
   */
  private static final String KEY_ROLLING_WINDOW_SIZE = "fs.azure.metrics.rolling.window.size";

  private final MetricsRegistry registry =
      new MetricsRegistry("azureFileSystem")
      .setContext("azureFileSystem");
  private final MutableCounterLong numberOfWebResponses =
      registry.newCounter(
          WASB_WEB_RESPONSES,
          "Total number of web responses obtained from Azure Storage",
          0L);
  private AtomicLong inMemoryNumberOfWebResponses = new AtomicLong(0);
  private final MutableCounterLong numberOfFilesCreated =
      registry.newCounter(
          WASB_FILES_CREATED,
          "Total number of files created through the WASB file system.",
          0L);
  private final MutableCounterLong numberOfFilesDeleted =
      registry.newCounter(
          WASB_FILES_DELETED,
          "Total number of files deleted through the WASB file system.",
          0L);
  private final MutableCounterLong numberOfDirectoriesCreated =
      registry.newCounter(
          WASB_DIRECTORIES_CREATED,
          "Total number of directories created through the WASB file system.",
          0L);
  private final MutableCounterLong numberOfDirectoriesDeleted =
      registry.newCounter(
          WASB_DIRECTORIES_DELETED,
          "Total number of directories deleted through the WASB file system.",
          0L);
  private final MutableGaugeLong bytesWrittenInLastSecond =
      registry.newGauge(
          WASB_BYTES_WRITTEN,
          "Total number of bytes written to Azure Storage during the last second.",
          0L);
  private final MutableGaugeLong bytesReadInLastSecond =
      registry.newGauge(
          WASB_BYTES_READ,
          "Total number of bytes read from Azure Storage during the last second.",
          0L);
  private final MutableGaugeLong maximumUploadBytesPerSecond =
      registry.newGauge(
          WASB_UPLOAD_RATE,
          "The maximum upload rate encountered to Azure Storage in bytes/second.",
          0L);
  private final MutableGaugeLong maximumDownloadBytesPerSecond =
      registry.newGauge(
          WASB_DOWNLOAD_RATE,
          "The maximum download rate encountered to Azure Storage in bytes/second.",
          0L);
  private final MutableCounterLong rawBytesUploaded =
      registry.newCounter(
          WASB_RAW_BYTES_UPLOADED,
          "Total number of raw bytes (including overhead) uploaded to Azure" 
          + " Storage.",
          0L);
  private final MutableCounterLong rawBytesDownloaded =
      registry.newCounter(
          WASB_RAW_BYTES_DOWNLOADED,
          "Total number of raw bytes (including overhead) downloaded from Azure" 
          + " Storage.",
          0L);
  private final MutableCounterLong clientErrors =
      registry.newCounter(
          WASB_CLIENT_ERRORS,
          "Total number of client-side errors by WASB (excluding 404).",
          0L);
  private final MutableCounterLong serverErrors =
      registry.newCounter(
          WASB_SERVER_ERRORS,
          "Total number of server-caused errors by WASB.",
          0L);
  private final MutableGaugeLong averageBlockUploadLatencyMs;
  private final MutableGaugeLong averageBlockDownloadLatencyMs;
  private long currentMaximumUploadBytesPerSecond;
  private long currentMaximumDownloadBytesPerSecond;
  private static final int DEFAULT_LATENCY_ROLLING_AVERAGE_WINDOW =
      5; // seconds
  private final RollingWindowAverage currentBlockUploadLatency;
  private final RollingWindowAverage currentBlockDownloadLatency;
  private UUID fileSystemInstanceId;

  public AzureFileSystemInstrumentation(Configuration conf) {
    fileSystemInstanceId = UUID.randomUUID();
    registry.tag("wasbFileSystemId",
        "A unique identifier for the file ",
        fileSystemInstanceId.toString());
    final int rollingWindowSizeInSeconds =
        conf.getInt(KEY_ROLLING_WINDOW_SIZE,
            DEFAULT_LATENCY_ROLLING_AVERAGE_WINDOW);
    averageBlockUploadLatencyMs =
        registry.newGauge(
            WASB_UPLOAD_LATENCY,
            String.format("The average latency in milliseconds of uploading a single block" 
            + ". The average latency is calculated over a %d-second rolling" 
            + " window.", rollingWindowSizeInSeconds),
            0L);
    averageBlockDownloadLatencyMs =
        registry.newGauge(
            WASB_DOWNLOAD_LATENCY,
            String.format("The average latency in milliseconds of downloading a single block" 
            + ". The average latency is calculated over a %d-second rolling" 
            + " window.", rollingWindowSizeInSeconds),
            0L);
    currentBlockUploadLatency =
        new RollingWindowAverage(rollingWindowSizeInSeconds * 1000);
    currentBlockDownloadLatency =
        new RollingWindowAverage(rollingWindowSizeInSeconds * 1000);
  }

  /**
   * The unique identifier for this file system in the metrics.
   */
  public UUID getFileSystemInstanceId() {
    return fileSystemInstanceId;
  }
  
  /**
   * Get the metrics registry information.
   */
  public MetricsInfo getMetricsRegistryInfo() {
    return registry.info();
  }

  /**
   * Sets the account name to tag all the metrics with.
   * @param accountName The account name.
   */
  public void setAccountName(String accountName) {
    registry.tag("accountName",
        "Name of the Azure Storage account that these metrics are going against",
        accountName);
  }

  /**
   * Sets the container name to tag all the metrics with.
   * @param containerName The container name.
   */
  public void setContainerName(String containerName) {
    registry.tag("containerName",
        "Name of the Azure Storage container that these metrics are going against",
        containerName);
  }

  /**
   * Indicate that we just got a web response from Azure Storage. This should
   * be called for every web request/response we do (to get accurate metrics
   * of how we're hitting the storage service).
   */
  public void webResponse() {
    numberOfWebResponses.incr();
    inMemoryNumberOfWebResponses.incrementAndGet();
  }

  /**
   * Gets the current number of web responses obtained from Azure Storage.
   * @return The number of web responses.
   */
  public long getCurrentWebResponses() {
    return inMemoryNumberOfWebResponses.get();
  }

  /**
   * Indicate that we just created a file through WASB.
   */
  public void fileCreated() {
    numberOfFilesCreated.incr();
  }

  /**
   * Indicate that we just deleted a file through WASB.
   */
  public void fileDeleted() {
    numberOfFilesDeleted.incr();
  }

  /**
   * Indicate that we just created a directory through WASB.
   */
  public void directoryCreated() {
    numberOfDirectoriesCreated.incr();
  }

  /**
   * Indicate that we just deleted a directory through WASB.
   */
  public void directoryDeleted() {
    numberOfDirectoriesDeleted.incr();
  }

  /**
   * Sets the current gauge value for how many bytes were written in the last
   *  second.
   * @param currentBytesWritten The number of bytes.
   */
  public void updateBytesWrittenInLastSecond(long currentBytesWritten) {
    bytesWrittenInLastSecond.set(currentBytesWritten);
  }

  /**
   * Sets the current gauge value for how many bytes were read in the last
   *  second.
   * @param currentBytesRead The number of bytes.
   */
  public void updateBytesReadInLastSecond(long currentBytesRead) {
    bytesReadInLastSecond.set(currentBytesRead);
  }

  /**
   * Record the current bytes-per-second upload rate seen.
   * @param bytesPerSecond The bytes per second.
   */
  public synchronized void currentUploadBytesPerSecond(long bytesPerSecond) {
    if (bytesPerSecond > currentMaximumUploadBytesPerSecond) {
      currentMaximumUploadBytesPerSecond = bytesPerSecond;
      maximumUploadBytesPerSecond.set(bytesPerSecond);
    }
  }

  /**
   * Record the current bytes-per-second download rate seen.
   * @param bytesPerSecond The bytes per second.
   */
  public synchronized void currentDownloadBytesPerSecond(long bytesPerSecond) {
    if (bytesPerSecond > currentMaximumDownloadBytesPerSecond) {
      currentMaximumDownloadBytesPerSecond = bytesPerSecond;
      maximumDownloadBytesPerSecond.set(bytesPerSecond);
    }
  }

  /**
   * Indicate that we just uploaded some data to Azure storage.
   * @param numberOfBytes The raw number of bytes uploaded (including overhead).
   */
  public void rawBytesUploaded(long numberOfBytes) {
    rawBytesUploaded.incr(numberOfBytes);
  }

  /**
   * Indicate that we just downloaded some data to Azure storage.
   * @param numberOfBytes The raw number of bytes downloaded (including overhead).
   */
  public void rawBytesDownloaded(long numberOfBytes) {
    rawBytesDownloaded.incr(numberOfBytes);
  }

  /**
   * Indicate that we just uploaded a block and record its latency.
   * @param latency The latency in milliseconds.
   */
  public void blockUploaded(long latency) {
    currentBlockUploadLatency.addPoint(latency);
  }

  /**
   * Indicate that we just downloaded a block and record its latency.
   * @param latency The latency in milliseconds.
   */
  public void blockDownloaded(long latency) {
    currentBlockDownloadLatency.addPoint(latency);
  }

  /**
   * Indicate that we just encountered a client-side error.
   */
  public void clientErrorEncountered() {
    clientErrors.incr();
  }

  /**
   * Indicate that we just encountered a server-caused error.
   */
  public void serverErrorEncountered() {
    serverErrors.incr();
  }

  /**
   * Get the current rolling average of the upload latency.
   * @return rolling average of upload latency in milliseconds.
   */
  public long getBlockUploadLatency() {
    return currentBlockUploadLatency.getCurrentAverage();
  }

  /**
   * Get the current rolling average of the download latency.
   * @return rolling average of download latency in milliseconds.
   */
  public long getBlockDownloadLatency() {
    return currentBlockDownloadLatency.getCurrentAverage();
  }

  /**
   * Get the current maximum upload bandwidth.
   * @return maximum upload bandwidth in bytes per second.
   */
  public long getCurrentMaximumUploadBandwidth() {
    return currentMaximumUploadBytesPerSecond;
  }

  /**
   * Get the current maximum download bandwidth.
   * @return maximum download bandwidth in bytes per second.
   */
  public long getCurrentMaximumDownloadBandwidth() {
    return currentMaximumDownloadBytesPerSecond;
  }

  @Override
  public void getMetrics(MetricsCollector builder, boolean all) {
    averageBlockDownloadLatencyMs.set(
        currentBlockDownloadLatency.getCurrentAverage());
    averageBlockUploadLatencyMs.set(
        currentBlockUploadLatency.getCurrentAverage());
    registry.snapshot(builder.addRecord(registry.info().name()), true);
  }
}
