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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;

public class AbfsReadFooterMetrics {
  private final AtomicBoolean isParquetFile;
  private final AtomicBoolean isParquetEvaluated;
  private final AtomicBoolean isLenUpdated;
  private String sizeReadByFirstRead;
  private String offsetDiffBetweenFirstAndSecondRead;
  private final AtomicLong fileLength;
  private double avgFileLength;
  private double avgReadLenRequested;
  private final AtomicBoolean collectMetrics;
  private final AtomicBoolean collectMetricsForNextRead;
  private final AtomicBoolean collectLenMetrics;
  private final AtomicLong dataLenRequested;
  private final AtomicLong offsetOfFirstRead;
  private final AtomicInteger readCount;
  private final ConcurrentSkipListMap<String, AbfsReadFooterMetrics> metricsMap;
  private static final String FOOTER_LENGTH = "20";

  public AbfsReadFooterMetrics() {
    this.isParquetFile = new AtomicBoolean(false);
    this.isParquetEvaluated = new AtomicBoolean(false);
    this.isLenUpdated = new AtomicBoolean(false);
    this.fileLength = new AtomicLong();
    this.readCount = new AtomicInteger(0);
    this.offsetOfFirstRead = new AtomicLong();
    this.collectMetrics = new AtomicBoolean(false);
    this.collectMetricsForNextRead = new AtomicBoolean(false);
    this.collectLenMetrics = new AtomicBoolean(false);
    this.dataLenRequested = new AtomicLong(0);
    this.metricsMap = new ConcurrentSkipListMap<>();
  }

  public Map<String, AbfsReadFooterMetrics> getMetricsMap() {
    return metricsMap;
  }

  private boolean getIsParquetFile() {
    return isParquetFile.get();
  }

  public void setIsParquetFile(boolean isParquetFile) {
    this.isParquetFile.set(isParquetFile);
  }

  private String getSizeReadByFirstRead() {
    return sizeReadByFirstRead;
  }

  public void setSizeReadByFirstRead(final String sizeReadByFirstRead) {
    this.sizeReadByFirstRead = sizeReadByFirstRead;
  }

  private String getOffsetDiffBetweenFirstAndSecondRead() {
    return offsetDiffBetweenFirstAndSecondRead;
  }

  public void setOffsetDiffBetweenFirstAndSecondRead(final String offsetDiffBetweenFirstAndSecondRead) {
    this.offsetDiffBetweenFirstAndSecondRead
        = offsetDiffBetweenFirstAndSecondRead;
  }

  private long getFileLength() {
    return fileLength.get();
  }

  private void setFileLength(long fileLength) {
    this.fileLength.set(fileLength);
  }

  private double getAvgFileLength() {
    return avgFileLength;
  }

  public void setAvgFileLength(final double avgFileLength) {
    this.avgFileLength = avgFileLength;
  }

  private double getAvgReadLenRequested() {
    return avgReadLenRequested;
  }

  public void setAvgReadLenRequested(final double avgReadLenRequested) {
    this.avgReadLenRequested = avgReadLenRequested;
  }

  private boolean getCollectMetricsForNextRead() {
    return collectMetricsForNextRead.get();
  }

  private void setCollectMetricsForNextRead(boolean collectMetricsForNextRead) {
    this.collectMetricsForNextRead.set(collectMetricsForNextRead);
  }

  private long getOffsetOfFirstRead() {
    return offsetOfFirstRead.get();
  }

  private void setOffsetOfFirstRead(long offsetOfFirstRead) {
    this.offsetOfFirstRead.set(offsetOfFirstRead);
  }

  private int getReadCount() {
    return readCount.get();
  }

  private void setReadCount(int readCount) {
    this.readCount.set(readCount);
  }

  private int incrementReadCount() {
    this.readCount.incrementAndGet();
    return getReadCount();
  }

  private boolean getCollectLenMetrics() {
    return collectLenMetrics.get();
  }

  private void setCollectLenMetrics(boolean collectLenMetrics) {
    this.collectLenMetrics.set(collectLenMetrics);

  }

  private long getDataLenRequested() {
    return dataLenRequested.get();
  }

  private void setDataLenRequested(long dataLenRequested) {
    this.dataLenRequested.set(dataLenRequested);
  }

  private void updateDataLenRequested(long dataLenRequested){
    this.dataLenRequested.addAndGet(dataLenRequested);
  }

  private boolean getCollectMetrics() {
    return collectMetrics.get();
  }

  private void setCollectMetrics(boolean collectMetrics) {
    this.collectMetrics.set(collectMetrics);
  }

  private boolean getIsParquetEvaluated() {
    return isParquetEvaluated.get();
  }

  private void setIsParquetEvaluated(boolean isParquetEvaluated) {
    this.isParquetEvaluated.set(isParquetEvaluated);
  }

  private boolean getIsLenUpdated() {
    return isLenUpdated.get();
  }

  private void setIsLenUpdated(boolean isLenUpdated) {
    this.isLenUpdated.set(isLenUpdated);
  }

  /**
   * Updates the metrics map with an entry for the specified file if it doesn't already exist.
   *
   * @param filePathIdentifier The unique identifier for the file.
   */
  public void updateMap(String filePathIdentifier) {
    // If the file is not already in the metrics map, add it with a new AbfsReadFooterMetrics object.
    metricsMap.computeIfAbsent(filePathIdentifier, key -> new AbfsReadFooterMetrics());
  }

  /**
   * Checks and updates metrics for a specific file identified by filePathIdentifier.
   * If the metrics do not exist for the file, they are initialized.
   *
   * @param filePathIdentifier The unique identifier for the file.
   * @param len               The length of the read operation.
   * @param contentLength     The total content length of the file.
   * @param nextReadPos       The position of the next read operation.
   */
  public void checkMetricUpdate(final String filePathIdentifier, final int len, final long contentLength,
      final long nextReadPos) {
    AbfsReadFooterMetrics readFooterMetrics = metricsMap.computeIfAbsent(
            filePathIdentifier, key -> new AbfsReadFooterMetrics());
    if (readFooterMetrics.getReadCount() == 0
        || (readFooterMetrics.getReadCount() >= 1
        && readFooterMetrics.getCollectMetrics())) {
      updateMetrics(filePathIdentifier, len, contentLength, nextReadPos);
    }
  }

  /**
   * Updates metrics for a specific file identified by filePathIdentifier.
   *
   * @param filePathIdentifier  The unique identifier for the file.
   * @param len                The length of the read operation.
   * @param contentLength      The total content length of the file.
   * @param nextReadPos        The position of the next read operation.
   */
  private void updateMetrics(final String filePathIdentifier, final int len, final long contentLength,
                             final long nextReadPos) {
    AbfsReadFooterMetrics readFooterMetrics = metricsMap.get(filePathIdentifier);

    // Create a new AbfsReadFooterMetrics object if it doesn't exist in the metricsMap.
    if (readFooterMetrics == null) {
      readFooterMetrics = new AbfsReadFooterMetrics();
      metricsMap.put(filePathIdentifier, readFooterMetrics);
    }

    int readCount;
    synchronized (this) {
      readCount = readFooterMetrics.incrementReadCount();
    }

    if (readCount == 1) {
      // Update metrics for the first read.
      updateMetricsOnFirstRead(readFooterMetrics, nextReadPos, len, contentLength);
    }

    synchronized (this) {
      if (readFooterMetrics.getCollectLenMetrics()) {
        readFooterMetrics.updateDataLenRequested(len);
      }
    }

    if (readCount == 2) {
      // Update metrics for the second read.
      updateMetricsOnSecondRead(readFooterMetrics, nextReadPos, len);
    }
  }

  /**
   * Updates metrics for the first read operation.
   *
   * @param readFooterMetrics The metrics object to update.
   * @param nextReadPos       The position of the next read operation.
   * @param len               The length of the read operation.
   * @param contentLength     The total content length of the file.
   */
  private void updateMetricsOnFirstRead(AbfsReadFooterMetrics readFooterMetrics, long nextReadPos, int len, long contentLength) {
    if (nextReadPos >= contentLength - (long) Integer.parseInt(FOOTER_LENGTH) * ONE_KB) {
      readFooterMetrics.setCollectMetrics(true);
      readFooterMetrics.setCollectMetricsForNextRead(true);
      readFooterMetrics.setOffsetOfFirstRead(nextReadPos);
      readFooterMetrics.setSizeReadByFirstRead(len + "_" + Math.abs(contentLength - nextReadPos));
      readFooterMetrics.setFileLength(contentLength);
    }
  }

  /**
   * Updates metrics for the second read operation.
   *
   * @param readFooterMetrics The metrics object to update.
   * @param nextReadPos       The position of the next read operation.
   * @param len               The length of the read operation.
   */
  private void updateMetricsOnSecondRead(AbfsReadFooterMetrics readFooterMetrics, long nextReadPos, int len) {
    if (readFooterMetrics.getCollectMetricsForNextRead()) {
      long offsetDiff = Math.abs(nextReadPos - readFooterMetrics.getOffsetOfFirstRead());
      readFooterMetrics.setOffsetDiffBetweenFirstAndSecondRead(len + "_" + offsetDiff);
      readFooterMetrics.setCollectLenMetrics(true);
    }
  }


  /**
   * Check if the given file should be marked as a Parquet file.
   *
   * @param metrics The metrics to evaluate.
   * @return True if the file meet the criteria for being marked as a Parquet file, false otherwise.
   */
  private boolean shouldMarkAsParquet(AbfsReadFooterMetrics metrics) {
    return metrics.getCollectMetrics()
            && metrics.getReadCount() >= 2
            && !metrics.getIsParquetEvaluated()
            && haveEqualValues(metrics.getSizeReadByFirstRead())
            && haveEqualValues(metrics.getOffsetDiffBetweenFirstAndSecondRead());
  }

  /**
   * Check if two values are equal, considering they are in the format "value1_value2".
   *
   * @param value The value to check.
   * @return True if the two parts of the value are equal, false otherwise.
   */
  private boolean haveEqualValues(String value) {
    String[] parts = value.split("_");
    return parts.length == 2 && parts[0].equals(parts[1]);
  }

  /**
   * Mark the given metrics as a Parquet file and update related values.
   *
   * @param metrics The metrics to mark as Parquet.
   */
  private void markAsParquet(AbfsReadFooterMetrics metrics) {
    metrics.setIsParquetFile(true);
    String[] parts = metrics.getSizeReadByFirstRead().split("_");
    metrics.setSizeReadByFirstRead(parts[0]);
    parts = metrics.getOffsetDiffBetweenFirstAndSecondRead().split("_");
    metrics.setOffsetDiffBetweenFirstAndSecondRead(parts[0]);
    metrics.setIsParquetEvaluated(true);
  }

  /**
   * Check each metric in the provided map and mark them as Parquet files if they meet the criteria.
   *
   * @param metricsMap The map containing metrics to evaluate.
   */
  public void checkIsParquet(Map<String, AbfsReadFooterMetrics> metricsMap) {
    for (Map.Entry<String, AbfsReadFooterMetrics> entry : metricsMap.entrySet()) {
      AbfsReadFooterMetrics readFooterMetrics = entry.getValue();
      if (shouldMarkAsParquet(readFooterMetrics)) {
        markAsParquet(readFooterMetrics);
        metricsMap.replace(entry.getKey(), readFooterMetrics);
      }
    }
  }

  /**
   * Updates the average read length requested for metrics of all files in the metrics map.
   * If the metrics indicate that the update is needed, it calculates the average read length and updates the metrics.
   *
   * @param metricsMap A map containing metrics for different files with unique identifiers.
   */
  private void updateLenRequested(Map<String, AbfsReadFooterMetrics> metricsMap) {
    for (AbfsReadFooterMetrics readFooterMetrics : metricsMap.values()) {
      if (shouldUpdateLenRequested(readFooterMetrics)) {
        int readReqCount = readFooterMetrics.getReadCount() - 2;
        readFooterMetrics.setAvgReadLenRequested(
                (double) readFooterMetrics.getDataLenRequested() / readReqCount);
        readFooterMetrics.setIsLenUpdated(true);
      }
    }
  }

  /**
   * Checks whether the average read length requested should be updated for the given metrics.
   *
   * The method returns true if the following conditions are met:
   * - Metrics collection is enabled.
   * - The number of read counts is greater than 2.
   * - The average read length has not been updated previously.
   *
   * @param readFooterMetrics The metrics object to evaluate.
   * @return True if the average read length should be updated, false otherwise.
   */
  private boolean shouldUpdateLenRequested(AbfsReadFooterMetrics readFooterMetrics) {
    return readFooterMetrics.getCollectMetrics()
            && readFooterMetrics.getReadCount() > 2
            && !readFooterMetrics.getIsLenUpdated();
  }

  /**
   * Calculates the average metrics from a list of AbfsReadFooterMetrics and sets the values in the provided 'avgParquetReadFooterMetrics' object.
   *
   * @param isParquetList The list of AbfsReadFooterMetrics to compute the averages from.
   * @param avgParquetReadFooterMetrics The target AbfsReadFooterMetrics object to store the computed average values.
   *
   * This method calculates various average metrics from the provided list and sets them in the 'avgParquetReadFooterMetrics' object.
   * The metrics include:
   * - Size read by the first read
   * - Offset difference between the first and second read
   * - Average file length
   * - Average requested read length
   */
  private void getParquetReadFooterMetricsAverage(List<AbfsReadFooterMetrics> isParquetList,
      AbfsReadFooterMetrics avgParquetReadFooterMetrics){
    avgParquetReadFooterMetrics.setSizeReadByFirstRead(
        String.format("%.3f", isParquetList.stream()
            .map(AbfsReadFooterMetrics::getSizeReadByFirstRead).mapToDouble(
                Double::parseDouble).average().orElse(0.0)));
    avgParquetReadFooterMetrics.setOffsetDiffBetweenFirstAndSecondRead(
        String.format("%.3f", isParquetList.stream()
            .map(AbfsReadFooterMetrics::getOffsetDiffBetweenFirstAndSecondRead)
            .mapToDouble(Double::parseDouble).average().orElse(0.0)));
    avgParquetReadFooterMetrics.setAvgFileLength(isParquetList.stream()
        .mapToDouble(AbfsReadFooterMetrics::getFileLength).average().orElse(0.0));
    avgParquetReadFooterMetrics.setAvgReadLenRequested(isParquetList.stream().
        map(AbfsReadFooterMetrics::getAvgReadLenRequested).
        mapToDouble(Double::doubleValue).average().orElse(0.0));
  }

  /**
   * Calculates the average metrics from a list of non-Parquet AbfsReadFooterMetrics instances.
   *
   * This method takes a list of AbfsReadFooterMetrics representing non-Parquet reads and calculates
   * the average values for the size read by the first read and the offset difference between the first
   * and second read. The averages are then set in the provided AbfsReadFooterMetrics instance.
   *
   * @param isNonParquetList A list of AbfsReadFooterMetrics instances representing non-Parquet reads.
   * @param avgNonParquetReadFooterMetrics The AbfsReadFooterMetrics instance to store the calculated averages.
   *                                      It is assumed that the size of the list is at least 1, and the first
   *                                      element of the list is used to determine the size of arrays.
   *                                      The instance is modified in-place with the calculated averages.
   *
   *
   **/
  private void getNonParquetReadFooterMetricsAverage(List<AbfsReadFooterMetrics> isNonParquetList,
                                                     AbfsReadFooterMetrics avgNonParquetReadFooterMetrics) {
    int size = isNonParquetList.get(0).getSizeReadByFirstRead().split("_").length;
    double[] store = new double[2 * size];
    // Calculating sum of individual values
    isNonParquetList.forEach(abfsReadFooterMetrics -> {
      String[] firstReadSize = abfsReadFooterMetrics.getSizeReadByFirstRead().split("_");
      String[] offDiffFirstSecondRead = abfsReadFooterMetrics.getOffsetDiffBetweenFirstAndSecondRead().split("_");

      for (int i = 0; i < firstReadSize.length; i++) {
        store[i] += Long.parseLong(firstReadSize[i]);
        store[i + size] += Long.parseLong(offDiffFirstSecondRead[i]);
      }
    });

    // Calculating averages and creating formatted strings
    StringJoiner firstReadSize = new StringJoiner("_");
    StringJoiner offDiffFirstSecondRead = new StringJoiner("_");

    for (int j = 0; j < size; j++) {
      firstReadSize.add(String.format("%.3f", store[j] / isNonParquetList.size()));
      offDiffFirstSecondRead.add(String.format("%.3f", store[j + size] / isNonParquetList.size()));
    }

    avgNonParquetReadFooterMetrics.setSizeReadByFirstRead(firstReadSize.toString());
    avgNonParquetReadFooterMetrics.setOffsetDiffBetweenFirstAndSecondRead(offDiffFirstSecondRead.toString());
    avgNonParquetReadFooterMetrics.setAvgFileLength(isNonParquetList.stream()
            .mapToDouble(AbfsReadFooterMetrics::getFileLength).average().orElse(0.0));
    avgNonParquetReadFooterMetrics.setAvgReadLenRequested(isNonParquetList.stream()
            .mapToDouble(AbfsReadFooterMetrics::getAvgReadLenRequested).average().orElse(0.0));
  }

  /*
  Acronyms:
  1.FR :- First Read (In case of parquet we only maintain the size requested by application for
  the first read, in case of non parquet we maintain a string separated by "_" delimiter where the first
  substring represents the len requested for first read and the second substring represents the seek pointer difference from the
  end of the file.)
  2.SR :- Second Read (In case of parquet we only maintain the size requested by application for
  the second read, in case of non parquet we maintain a string separated by "_" delimiter where the first
  substring represents the len requested for second read and the second substring represents the seek pointer difference from the
  offset of the first read.)
  3.FL :- Total length of the file requested for read
   */
  public String getReadFooterMetrics(AbfsReadFooterMetrics avgReadFooterMetrics) {
    String readFooterMetric = "";
    if (avgReadFooterMetrics.getIsParquetFile()) {
      readFooterMetric += "$Parquet:";
    } else {
      readFooterMetric += "$NonParquet:";
    }
    readFooterMetric += "$FR=" + avgReadFooterMetrics.getSizeReadByFirstRead()
        + "$SR="
        + avgReadFooterMetrics.getOffsetDiffBetweenFirstAndSecondRead()
        + "$FL=" + String.format("%.3f",
        avgReadFooterMetrics.getAvgFileLength())
        + "$RL=" + String.format("%.3f",
        avgReadFooterMetrics.getAvgReadLenRequested());
    return readFooterMetric;
  }

/**
 * Retrieves and aggregates read footer metrics for both Parquet and non-Parquet files from a list
 * of AbfsReadFooterMetrics instances. The function calculates the average metrics separately for
 * Parquet and non-Parquet files and returns a formatted string containing the aggregated metrics.
 *
 * @param readFooterMetricsList A list of AbfsReadFooterMetrics instances containing read footer metrics
 *                              for both Parquet and non-Parquet files.
 *
 * @return A formatted string containing the aggregated read footer metrics for both Parquet and non-Parquet files.
 *
 **/
private String getFooterMetrics(List<AbfsReadFooterMetrics> readFooterMetricsList) {
  List<AbfsReadFooterMetrics> isParquetList = new ArrayList<>();
  List<AbfsReadFooterMetrics> isNonParquetList = new ArrayList<>();
  for (AbfsReadFooterMetrics abfsReadFooterMetrics : readFooterMetricsList) {
    if (abfsReadFooterMetrics.getIsParquetFile()) {
      isParquetList.add(abfsReadFooterMetrics);
    } else {
      if (abfsReadFooterMetrics.getReadCount() >= 2) {
        isNonParquetList.add(abfsReadFooterMetrics);
      }
    }
  }
  AbfsReadFooterMetrics avgParquetReadFooterMetrics = new AbfsReadFooterMetrics();
  AbfsReadFooterMetrics avgNonparquetReadFooterMetrics = new AbfsReadFooterMetrics();
  String readFooterMetric = "";
  if (!isParquetList.isEmpty()) {
    avgParquetReadFooterMetrics.setIsParquetFile(true);
    getParquetReadFooterMetricsAverage(isParquetList, avgParquetReadFooterMetrics);
    readFooterMetric += getReadFooterMetrics(avgParquetReadFooterMetrics);
  }
  if (!isNonParquetList.isEmpty()) {
    avgNonparquetReadFooterMetrics.setIsParquetFile(false);
    getNonParquetReadFooterMetricsAverage(isNonParquetList, avgNonparquetReadFooterMetrics);
    readFooterMetric += getReadFooterMetrics(avgNonparquetReadFooterMetrics);
  }
  return readFooterMetric;
}


  @Override
  public String toString() {
    Map<String, AbfsReadFooterMetrics> metricsMap = getMetricsMap();
    List<AbfsReadFooterMetrics> readFooterMetricsList = new ArrayList<>();
    if (metricsMap != null && !(metricsMap.isEmpty())) {
      checkIsParquet(metricsMap);
      updateLenRequested(metricsMap);
      for (Map.Entry<String, AbfsReadFooterMetrics> entry : metricsMap.entrySet()) {
        AbfsReadFooterMetrics abfsReadFooterMetrics = entry.getValue();
        if (abfsReadFooterMetrics.getCollectMetrics()) {
          readFooterMetricsList.add(entry.getValue());
        }
      }
    }
    String readFooterMetrics = "";
    if (!readFooterMetricsList.isEmpty()) {
      readFooterMetrics = getFooterMetrics(readFooterMetricsList);
    }
    return readFooterMetrics;
  }
}

