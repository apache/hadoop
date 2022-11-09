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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;

public class AbfsReadFooterMetrics {
  private String filePath;
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
  private final Map<String, AbfsReadFooterMetrics> metricsMap;

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
    this.metricsMap = new ConcurrentHashMap<>();
  }

  public AbfsReadFooterMetrics(String filePath) {
    this();
    this.filePath = filePath;
  }

  public Map<String, AbfsReadFooterMetrics> getMetricsMap() {
    return metricsMap;
  }

  public AtomicBoolean getIsParquetFile() {
    return isParquetFile;
  }

  public String getSizeReadByFirstRead() {
    return sizeReadByFirstRead;
  }

  public void setSizeReadByFirstRead(final String sizeReadByFirstRead) {
    this.sizeReadByFirstRead = sizeReadByFirstRead;
  }

  public String getOffsetDiffBetweenFirstAndSecondRead() {
    return offsetDiffBetweenFirstAndSecondRead;
  }

  public void setOffsetDiffBetweenFirstAndSecondRead(final String offsetDiffBetweenFirstAndSecondRead) {
    this.offsetDiffBetweenFirstAndSecondRead
        = offsetDiffBetweenFirstAndSecondRead;
  }

  public AtomicLong getFileLength() {
    return fileLength;
  }

  public double getAvgFileLength() {
    return avgFileLength;
  }

  public void setAvgFileLength(final double avgFileLength) {
    this.avgFileLength = avgFileLength;
  }

  public double getAvgReadLenRequested() {
    return avgReadLenRequested;
  }

  public void setAvgReadLenRequested(final double avgReadLenRequested) {
    this.avgReadLenRequested = avgReadLenRequested;
  }

  public AtomicBoolean getCollectMetricsForNextRead() {
    return collectMetricsForNextRead;
  }

  public AtomicLong getOffsetOfFirstRead() {
    return offsetOfFirstRead;
  }

  public AtomicInteger getReadCount() {
    return readCount;
  }

  public AtomicBoolean getCollectLenMetrics() {
    return collectLenMetrics;
  }

  public AtomicLong getDataLenRequested() {
    return dataLenRequested;
  }

  public AtomicBoolean getCollectMetrics() {
    return collectMetrics;
  }

  public AtomicBoolean getIsParquetEvaluated() {
    return isParquetEvaluated;
  }

  public AtomicBoolean getIsLenUpdated() {
    return isLenUpdated;
  }

  public void checkIsParquet(Map<String, AbfsReadFooterMetrics> metricsMap) {
    for (Map.Entry<String, AbfsReadFooterMetrics> entry : metricsMap.entrySet()) //using map.entrySet() for iteration
    {
      AbfsReadFooterMetrics readFooterMetrics = entry.getValue();
      if (readFooterMetrics.getCollectMetrics().get() && readFooterMetrics.getReadCount().get() >= 2) {
        if (!readFooterMetrics.getIsParquetEvaluated().get()) {
          String[] firstReadSize = readFooterMetrics.getSizeReadByFirstRead()
              .split("_");
          String[] offDiffFirstSecondRead
              = readFooterMetrics.getOffsetDiffBetweenFirstAndSecondRead()
              .split("_");
          if ((firstReadSize[0].equals(firstReadSize[1]))
              && (offDiffFirstSecondRead[0].equals(
              offDiffFirstSecondRead[1]))) {
            readFooterMetrics.getIsParquetFile().set(true);
            readFooterMetrics.setSizeReadByFirstRead(firstReadSize[0]);
            readFooterMetrics.setOffsetDiffBetweenFirstAndSecondRead(
                offDiffFirstSecondRead[0]);
          }
        }
        readFooterMetrics.getIsParquetEvaluated().set(true);
        metricsMap.replace(entry.getKey(), readFooterMetrics);
      }
    }
  }

  public void updateLenRequested(Map<String, AbfsReadFooterMetrics> metricsMap) {
    for (Map.Entry<String, AbfsReadFooterMetrics> entry : metricsMap.entrySet()) //using map.entrySet() for iteration
    {
      AbfsReadFooterMetrics readFooterMetrics = entry.getValue();
      if (readFooterMetrics.getCollectMetrics().get() && readFooterMetrics.getReadCount().get() > 2) {
        if (!readFooterMetrics.getIsLenUpdated().get()) {
            int readReqCount = readFooterMetrics.getReadCount().get() - 2;
            readFooterMetrics.setAvgReadLenRequested(
                (double) readFooterMetrics.getDataLenRequested().get()
                    / readReqCount);
            metricsMap.replace(entry.getKey(), readFooterMetrics);
        }
        readFooterMetrics.getIsLenUpdated().set(true);
        metricsMap.replace(entry.getKey(), readFooterMetrics);
      }
    }
  }

  public void getParquetReadFooterMetricsAverage(List<AbfsReadFooterMetrics> isParquetList,
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
        .map(AbfsReadFooterMetrics::getFileLength)
        .mapToDouble(AtomicLong::get).average().orElse(0.0));
    avgParquetReadFooterMetrics.setAvgReadLenRequested(isParquetList.stream().
        map(AbfsReadFooterMetrics::getAvgReadLenRequested).
        mapToDouble(Double::doubleValue).average().orElse(0.0));
  }

  public void getNonParquetReadFooterMetricsAverage(List<AbfsReadFooterMetrics> isNonParquetList,
      AbfsReadFooterMetrics avgNonParquetReadFooterMetrics){
    int size = isNonParquetList.get(0).getSizeReadByFirstRead().split("_").length;
    double[] store = new double[2*size];
    for (AbfsReadFooterMetrics abfsReadFooterMetrics : isNonParquetList) {
      String[] firstReadSize = abfsReadFooterMetrics.getSizeReadByFirstRead().split("_");
      String[] offDiffFirstSecondRead = abfsReadFooterMetrics.getOffsetDiffBetweenFirstAndSecondRead().split("_");
      for (int i = 0; i < firstReadSize.length; i++) {
        store[i] += Long.parseLong(firstReadSize[i]);
        store[i + size] += Long.parseLong(offDiffFirstSecondRead[i]);
      }
    }
    StringBuilder firstReadSize = new StringBuilder();
    StringBuilder offDiffFirstSecondRead = new StringBuilder();
    firstReadSize.append(String.format("%.3f", store[0] / isNonParquetList.size()));
    offDiffFirstSecondRead.append(String.format("%.3f", store[size] / isNonParquetList.size()));
    for (int j = 1; j < size; j++) {
      firstReadSize.append("_")
          .append(String.format("%.3f", store[j] / isNonParquetList.size()));
      offDiffFirstSecondRead.append("_")
          .append(String.format("%.3f", store[j + size] / isNonParquetList.size()));
    }
    avgNonParquetReadFooterMetrics.setSizeReadByFirstRead(firstReadSize.toString());
    avgNonParquetReadFooterMetrics.setOffsetDiffBetweenFirstAndSecondRead(offDiffFirstSecondRead.toString());
    avgNonParquetReadFooterMetrics.setAvgFileLength(isNonParquetList.stream()
        .map(AbfsReadFooterMetrics::getFileLength)
        .mapToDouble(AtomicLong::get).average().orElse(0.0));
    avgNonParquetReadFooterMetrics.setAvgReadLenRequested(isNonParquetList.stream().
            map(AbfsReadFooterMetrics::getAvgReadLenRequested).
            mapToDouble(Double::doubleValue).average().orElse(0.0));
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
    if (avgReadFooterMetrics.getIsParquetFile().get()) {
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

  public String getFooterMetrics(List<AbfsReadFooterMetrics> readFooterMetricsList, String readFooterMetric){
    List<AbfsReadFooterMetrics> isParquetList = new ArrayList<>();
    List<AbfsReadFooterMetrics> isNonParquetList = new ArrayList<>();
    for (AbfsReadFooterMetrics abfsReadFooterMetrics : readFooterMetricsList) {
      if (abfsReadFooterMetrics.getIsParquetFile().get()) {
        isParquetList.add(abfsReadFooterMetrics);
      } else {
        isNonParquetList.add(abfsReadFooterMetrics);
      }
    }
    AbfsReadFooterMetrics avgParquetReadFooterMetrics = new AbfsReadFooterMetrics();
    AbfsReadFooterMetrics avgNonparquetReadFooterMetrics = new AbfsReadFooterMetrics();

    if (!isParquetList.isEmpty()){
      avgParquetReadFooterMetrics.getIsParquetFile().set(true);
      getParquetReadFooterMetricsAverage(isParquetList, avgParquetReadFooterMetrics);
      readFooterMetric += getReadFooterMetrics(avgParquetReadFooterMetrics);
    }
    if (!isNonParquetList.isEmpty()) {
      avgNonparquetReadFooterMetrics.getIsParquetFile().set(false);
      getNonParquetReadFooterMetricsAverage(isNonParquetList, avgNonparquetReadFooterMetrics);
      readFooterMetric += getReadFooterMetrics(avgNonparquetReadFooterMetrics);
    }
    return readFooterMetric + "";
  }

  @Override
  public String toString() {
      Map<String, AbfsReadFooterMetrics> metricsMap = getMetricsMap();
      List<AbfsReadFooterMetrics> readFooterMetricsList = new ArrayList<>();
      if (metricsMap != null && !(metricsMap.isEmpty())) {
        checkIsParquet(metricsMap);
        updateLenRequested(metricsMap);
        for (Map.Entry<String, AbfsReadFooterMetrics> entry : metricsMap.entrySet()) //using map.entrySet() for iteration
        {
          AbfsReadFooterMetrics abfsReadFooterMetrics = entry.getValue();
          if(abfsReadFooterMetrics.getCollectMetrics().get()) {
            readFooterMetricsList.add(entry.getValue());
          }
        }
      }
      String readFooterMetric = "";
      if (!readFooterMetricsList.isEmpty()) {
        readFooterMetric = getFooterMetrics(readFooterMetricsList, readFooterMetric);
      }
      return readFooterMetric;
    }
}

