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

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum;
import org.apache.hadoop.fs.azurebfs.statistics.AbstractAbfsStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.COUNTER;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.GAUGE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.PARQUET;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.NON_PARQUET;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.TOTAL_FILES;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.FILE_LENGTH;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.SIZE_READ_BY_FIRST_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.READ_LEN_REQUESTED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.READ_COUNT;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.FIRST_OFFSET_DIFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.SECOND_OFFSET_DIFF;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

public class AbfsReadFooterMetrics extends AbstractAbfsStatisticsSource {
    private static final String FOOTER_LENGTH = "20";

    private static class CheckFileType {
        private final AtomicBoolean collectMetrics;
        private final AtomicBoolean collectMetricsForNextRead;
        private final AtomicBoolean collectLenMetrics;
        private final AtomicLong readCount;
        private final AtomicLong offsetOfFirstRead;
        private Boolean isParquet = null;
        private String sizeReadByFirstRead;
        private String offsetDiffBetweenFirstAndSecondRead;

        private CheckFileType() {
            collectMetrics = new AtomicBoolean(false);
            collectMetricsForNextRead = new AtomicBoolean(false);
            collectLenMetrics = new AtomicBoolean(false);
            readCount = new AtomicLong(0);
            offsetOfFirstRead = new AtomicLong(0);
        }

        private boolean isParquet() {
            if (isParquet != null) return isParquet;
            isParquet = collectMetrics.get() && readCount.get() >= 2 &&
                    haveEqualValues(sizeReadByFirstRead) &&
                    haveEqualValues(offsetDiffBetweenFirstAndSecondRead);
            return isParquet;
        }

        private boolean haveEqualValues(String value) {
            String[] parts = value.split("_");
            return parts.length == 2 && parts[0].equals(parts[1]);
        }

        private void incrementReadCount() {
            readCount.incrementAndGet();
        }

        private long getReadCount() {
            return readCount.get();
        }

        private void setCollectMetrics(boolean collect) {
            collectMetrics.set(collect);
        }

        private boolean getCollectMetrics() {
            return collectMetrics.get();
        }

        private void setCollectMetricsForNextRead(boolean collect) {
            collectMetricsForNextRead.set(collect);
        }

        private boolean getCollectMetricsForNextRead() {
            return collectMetricsForNextRead.get();
        }

        private boolean getCollectLenMetrics() {
            return collectLenMetrics.get();
        }

        private void setCollectLenMetrics(boolean collect) {
            collectLenMetrics.set(collect);
        }

        private void setOffsetOfFirstRead(long offset) {
            offsetOfFirstRead.set(offset);
        }

        private long getOffsetOfFirstRead() {
            return offsetOfFirstRead.get();
        }

        private void setSizeReadByFirstRead(String size) {
            sizeReadByFirstRead = size;
        }

        private String getSizeReadByFirstRead() {
            return sizeReadByFirstRead;
        }

        private void setOffsetDiffBetweenFirstAndSecondRead(String offsetDiff) {
            offsetDiffBetweenFirstAndSecondRead = offsetDiff;
        }

        private String getOffsetDiffBetweenFirstAndSecondRead() {
            return offsetDiffBetweenFirstAndSecondRead;
        }
    }

    private final Map<String, CheckFileType> checkFileMap = new HashMap<>();

    public AbfsReadFooterMetrics() {
        IOStatisticsStore ioStatisticsStore = iostatisticsStore()
                .withCounters(getMetricNames(COUNTER))
                .withGauges(getMetricNames(GAUGE))
                .build();
        setIOStatistics(ioStatisticsStore);
    }

    private String[] getMetricNames(String type) {
        return Arrays.stream(AbfsReadFooterMetricsEnum.values())
                .filter(metric -> metric.getType().equals(type))
                .flatMap(metric -> Stream.of(PARQUET + COLON + metric.getName(), NON_PARQUET + COLON + metric.getName()))
                .toArray(String[]::new);
    }

    private long getMetricValue(String fileType, AbfsReadFooterMetricsEnum metric) {
        switch (metric.getType()) {
            case COUNTER:
                return lookupCounterValue(fileType + COLON + metric.getName());
            case GAUGE:
                return lookupGaugeValue(fileType + COLON + metric.getName());
            default:
                return 0;
        }
    }

    public void updateMetric(String fileType, AbfsReadFooterMetricsEnum metric, long value) {
        updateGauge(fileType + COLON + metric.getName(), value);
    }

    public void incrementMetricValue(String fileType, AbfsReadFooterMetricsEnum metricName) {
        incCounter(fileType + COLON + metricName.getName());
    }

    public void incrementMetricValue(String fileType, AbfsReadFooterMetricsEnum metricName, long value) {
        incCounter(fileType + COLON + metricName.getName(), value);
    }

    public Long getTotalFiles() {
        return getMetricValue(PARQUET, TOTAL_FILES) + getMetricValue(NON_PARQUET, TOTAL_FILES);
    }

    public void updateMap(String filePathIdentifier) {
        checkFileMap.computeIfAbsent(filePathIdentifier, key -> new CheckFileType());
    }

    public void checkMetricUpdate(final String filePathIdentifier, final int len, final long contentLength, final long nextReadPos) {
        CheckFileType checkFileType = checkFileMap.computeIfAbsent(filePathIdentifier, key -> new CheckFileType());
        if (checkFileType.getReadCount() == 0 || (checkFileType.getReadCount() >= 1 && checkFileType.getCollectMetrics())) {
            updateMetrics(checkFileType, len, contentLength, nextReadPos);
        }
    }

    private void updateMetrics(CheckFileType checkFileType, int len, long contentLength, long nextReadPos) {
        synchronized (this) {
            checkFileType.incrementReadCount();
        }

        long readCount = checkFileType.getReadCount();

        if (readCount == 1) {
            handleFirstRead(checkFileType, nextReadPos, len, contentLength);
        } else if (readCount == 2) {
            handleSecondRead(checkFileType, nextReadPos, len, contentLength);
        } else {
            handleFurtherRead(checkFileType, len);
        }
    }

    private void handleFirstRead(CheckFileType checkFileType, long nextReadPos, int len, long contentLength) {
        if (nextReadPos >= contentLength - (long) Integer.parseInt(FOOTER_LENGTH) * ONE_KB) {
            checkFileType.setCollectMetrics(true);
            checkFileType.setCollectMetricsForNextRead(true);
            checkFileType.setOffsetOfFirstRead(nextReadPos);
            checkFileType.setSizeReadByFirstRead(len + "_" + Math.abs(contentLength - nextReadPos));
        }
    }

    private void handleSecondRead(CheckFileType checkFileType, long nextReadPos, int len, long contentLength) {
        if (checkFileType.getCollectMetricsForNextRead()) {
            long offsetDiff = Math.abs(nextReadPos - checkFileType.getOffsetOfFirstRead());
            checkFileType.setOffsetDiffBetweenFirstAndSecondRead(len + "_" + offsetDiff);
            checkFileType.setCollectLenMetrics(true);
            updateMetricsData(checkFileType, len, contentLength);
        }
    }

    private void handleFurtherRead(CheckFileType checkFileType, int len) {
        if (checkFileType.getCollectLenMetrics()) {
            String fileType = checkFileType.isParquet() ? PARQUET : NON_PARQUET;
            updateMetric(fileType, READ_LEN_REQUESTED, len);
            incrementMetricValue(fileType, READ_COUNT);
        }
    }

    private void updateMetricsData(CheckFileType checkFileType, int len, long contentLength) {
        long sizeReadByFirstRead = Long.parseLong(checkFileType.getSizeReadByFirstRead().split("_")[0]);
        long firstOffsetDiff = Long.parseLong(checkFileType.getSizeReadByFirstRead().split("_")[1]);
        long secondOffsetDiff = Long.parseLong(checkFileType.getOffsetDiffBetweenFirstAndSecondRead().split("_")[1]);
        String fileType = checkFileType.isParquet() ? PARQUET : NON_PARQUET;

        incrementMetricValue(fileType, READ_COUNT, 2);
        updateMetric(fileType, READ_LEN_REQUESTED, len + sizeReadByFirstRead);
        updateMetric(fileType, FILE_LENGTH, contentLength);
        updateMetric(fileType, SIZE_READ_BY_FIRST_READ, sizeReadByFirstRead);
        updateMetric(fileType, OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ, len);
        updateMetric(fileType, FIRST_OFFSET_DIFF, firstOffsetDiff);
        updateMetric(fileType, SECOND_OFFSET_DIFF, secondOffsetDiff);
        incrementMetricValue(fileType, TOTAL_FILES);
    }

    public String getReadFooterMetrics(String fileType) {
        StringBuilder readFooterMetric = new StringBuilder();
        appendMetrics(readFooterMetric, fileType);
        return readFooterMetric.toString();
    }

    private void appendMetrics(StringBuilder metricBuilder, String fileType) {
        long totalFiles = getMetricValue(fileType, TOTAL_FILES);
        if (totalFiles <= 0) return;

        long readCount = getMetricValue(fileType, READ_COUNT);
        String sizeReadByFirstRead = String.format("%.3f",getMetricValue(fileType, SIZE_READ_BY_FIRST_READ) / (double) totalFiles);
        String offsetDiffBetweenFirstAndSecondRead = String.format("%.3f",getMetricValue(fileType, OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ) / (double) totalFiles);

        if (NON_PARQUET.equals(fileType)) {
            sizeReadByFirstRead += "_" + String.format("%.3f",getMetricValue(fileType, FIRST_OFFSET_DIFF) / (double) totalFiles);
            offsetDiffBetweenFirstAndSecondRead += "_" + String.format("%.3f",getMetricValue(fileType, SECOND_OFFSET_DIFF) / (double) totalFiles);
        }

        metricBuilder.append("$").append(fileType)
                .append(":$FR=").append(sizeReadByFirstRead)
                .append("$SR=").append(offsetDiffBetweenFirstAndSecondRead)
                .append("$FL=").append(String.format("%.3f", getMetricValue(fileType, FILE_LENGTH) / (double) totalFiles))
                .append("$RL=").append(String.format("%.3f", getMetricValue(fileType, READ_LEN_REQUESTED) / (double) (readCount - 2 * totalFiles)));
    }

    @Override
    public String toString() {
        StringBuilder readFooterMetric = new StringBuilder();
        appendMetrics(readFooterMetric, PARQUET);
        appendMetrics(readFooterMetric, NON_PARQUET);
        return readFooterMetric.toString();
    }
}
