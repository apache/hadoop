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

import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;

public class AbfsReadFooterMetrics {
  private boolean isParquetFile;
  private String sizeReadByFirstRead;
  private String offsetDiffBetweenFirstAndSecondRead;
  private final AtomicLong fileLength;
  private double avgFileLength;

  public AbfsReadFooterMetrics() {
    this.fileLength = new AtomicLong();
  }

  public boolean getIsParquetFile() {
    return isParquetFile;
  }

  public void setParquetFile(final boolean parquetFile) {
    isParquetFile = parquetFile;
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

  public static void getParquetReadFooterMetricsAverage(List<AbfsReadFooterMetrics> isParquetList,
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
  }

  public static void getNonParquetReadFooterMetricsAverage(List<AbfsReadFooterMetrics> isNonParquetList,
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
  public static String getReadFooterMetrics(AbfsReadFooterMetrics avgReadFooterMetrics) {
    String readFooterMetric = "";
    if (avgReadFooterMetrics.getIsParquetFile()) {
      readFooterMetric += "Parquet:";
    } else {
      readFooterMetric += "NonParquet:";
    }
    readFooterMetric += " #FR=" + avgReadFooterMetrics.getSizeReadByFirstRead()
        + " #SR="
        + avgReadFooterMetrics.getOffsetDiffBetweenFirstAndSecondRead()
        + " #FL=" + String.format("%.3f",
        avgReadFooterMetrics.getAvgFileLength());
    return readFooterMetric + " ";
  }

  public static String getFooterMetrics(List<AbfsReadFooterMetrics> readFooterMetricsList, String readFooterMetric){
    List<AbfsReadFooterMetrics> isParquetList = new ArrayList<>();
    List<AbfsReadFooterMetrics> isNonParquetList = new ArrayList<>();
    for (AbfsReadFooterMetrics abfsReadFooterMetrics : readFooterMetricsList) {
      if (abfsReadFooterMetrics.getIsParquetFile()) {
        isParquetList.add(abfsReadFooterMetrics);
      } else {
        isNonParquetList.add(abfsReadFooterMetrics);
      }
    }
    AbfsReadFooterMetrics avgParquetReadFooterMetrics = new AbfsReadFooterMetrics();
    AbfsReadFooterMetrics avgNonparquetReadFooterMetrics = new AbfsReadFooterMetrics();

    if (!isParquetList.isEmpty()){
      avgParquetReadFooterMetrics.setParquetFile(true);
      AbfsReadFooterMetrics.getParquetReadFooterMetricsAverage(isParquetList, avgParquetReadFooterMetrics);
      readFooterMetric += AbfsReadFooterMetrics.getReadFooterMetrics(avgParquetReadFooterMetrics);
    }
    if (!isNonParquetList.isEmpty()) {
      avgNonparquetReadFooterMetrics.setParquetFile(false);
      AbfsReadFooterMetrics.getNonParquetReadFooterMetricsAverage(isNonParquetList, avgNonparquetReadFooterMetrics);
      readFooterMetric += AbfsReadFooterMetrics.getReadFooterMetrics(avgNonparquetReadFooterMetrics);
    }
    return readFooterMetric;
  }
}
