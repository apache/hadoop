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

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum;
import org.apache.hadoop.fs.azurebfs.enums.FileType;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.azurebfs.statistics.AbstractAbfsStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.TOTAL_FILES;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.FILE_LENGTH;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.SIZE_READ_BY_FIRST_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.READ_LEN_REQUESTED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.READ_COUNT;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.FIRST_OFFSET_DIFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.SECOND_OFFSET_DIFF;
import static org.apache.hadoop.fs.azurebfs.enums.FileType.PARQUET;
import static org.apache.hadoop.fs.azurebfs.enums.FileType.NON_PARQUET;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_GAUGE;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

public class AbfsReadFooterMetrics extends AbstractAbfsStatisticsSource {
    private static final String FOOTER_LENGTH = "20";
    private static final List<FileType> FILE_TYPE_LIST = Arrays.asList(PARQUET, NON_PARQUET);

    private static final class CheckFileType {
        private final AtomicBoolean collectMetrics;
        private final AtomicBoolean collectMetricsForNextRead;
        private final AtomicBoolean collectLenMetrics;
        private final AtomicLong readCount;
        private final AtomicLong offsetOfFirstRead;
        private FileType fileType = null;
        private String sizeReadByFirstRead;
        private String offsetDiffBetweenFirstAndSecondRead;

        private CheckFileType() {
            collectMetrics = new AtomicBoolean(false);
            collectMetricsForNextRead = new AtomicBoolean(false);
            collectLenMetrics = new AtomicBoolean(false);
            readCount = new AtomicLong(0);
            offsetOfFirstRead = new AtomicLong(0);
        }

        private void updateFileType() {
            if (fileType == null) {
                fileType = collectMetrics.get() && readCount.get() >= 2 &&
                        haveEqualValues(sizeReadByFirstRead) &&
                        haveEqualValues(offsetDiffBetweenFirstAndSecondRead) ? PARQUET : NON_PARQUET;
            }
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

        private FileType getFileType() {
            return fileType;
        }
    }

    private final Map<String, CheckFileType> checkFileMap = new HashMap<>();

    public AbfsReadFooterMetrics() {
        IOStatisticsStore ioStatisticsStore = iostatisticsStore()
                .withCounters(getMetricNames(TYPE_COUNTER))
                .withGauges(getMetricNames(TYPE_GAUGE))
                .build();
        setIOStatistics(ioStatisticsStore);
    }

    private String[] getMetricNames(StatisticTypeEnum type) {
        return Arrays.stream(AbfsReadFooterMetricsEnum.values())
                .filter(readFooterMetricsEnum -> readFooterMetricsEnum.getStatisticType().equals(type))
                .flatMap(readFooterMetricsEnum ->
                        FILE.equals(readFooterMetricsEnum.getType()) ?
                                FILE_TYPE_LIST.stream().map(fileType -> fileType + COLON + readFooterMetricsEnum.getName()) :
                                Stream.of(readFooterMetricsEnum.getName()))
                .toArray(String[]::new);
    }

    private long getMetricValue(FileType fileType, AbfsReadFooterMetricsEnum metric) {
        switch (metric.getStatisticType()) {
            case TYPE_COUNTER:
                return lookupCounterValue(fileType + COLON + metric.getName());
            case TYPE_GAUGE:
                return lookupGaugeValue(fileType + COLON + metric.getName());
            default:
                return 0;
        }
    }

    public void updateMetricValue(FileType fileType, AbfsReadFooterMetricsEnum metric, long value) {
        updateGaugeValue(fileType + COLON + metric.getName(), value);
    }

    public void incrementMetricValue(FileType fileType, AbfsReadFooterMetricsEnum metricName) {
        incCounterValue(fileType + COLON + metricName.getName());
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
            checkFileType.updateFileType();
            updateMetricsData(checkFileType, len, contentLength);
        }
    }

    private void handleFurtherRead(CheckFileType checkFileType, int len) {
        if (checkFileType.getCollectLenMetrics() && checkFileType.getFileType() != null) {
            FileType fileType = checkFileType.getFileType();
            updateMetricValue(fileType, READ_LEN_REQUESTED, len);
            incrementMetricValue(fileType, READ_COUNT);
        }
    }

    private void updateMetricsData(CheckFileType checkFileType, int len, long contentLength) {
        long sizeReadByFirstRead = Long.parseLong(checkFileType.getSizeReadByFirstRead().split("_")[0]);
        long firstOffsetDiff = Long.parseLong(checkFileType.getSizeReadByFirstRead().split("_")[1]);
        long secondOffsetDiff = Long.parseLong(checkFileType.getOffsetDiffBetweenFirstAndSecondRead().split("_")[1]);
        FileType fileType = checkFileType.getFileType();

        updateMetricValue(fileType, READ_LEN_REQUESTED, len + sizeReadByFirstRead);
        updateMetricValue(fileType, FILE_LENGTH, contentLength);
        updateMetricValue(fileType, SIZE_READ_BY_FIRST_READ, sizeReadByFirstRead);
        updateMetricValue(fileType, OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ, len);
        updateMetricValue(fileType, FIRST_OFFSET_DIFF, firstOffsetDiff);
        updateMetricValue(fileType, SECOND_OFFSET_DIFF, secondOffsetDiff);
        incrementMetricValue(fileType, TOTAL_FILES);
    }

    public String getReadFooterMetrics(FileType fileType) {
        StringBuilder readFooterMetric = new StringBuilder();
        appendMetrics(readFooterMetric, fileType);
        return readFooterMetric.toString();
    }

    private void appendMetrics(StringBuilder metricBuilder, FileType fileType) {
        long totalFiles = getMetricValue(fileType, TOTAL_FILES);
        if (totalFiles <= 0) return;

        long readCount = getMetricValue(fileType, READ_COUNT);
        String sizeReadByFirstRead = String.format("%.3f", getMetricValue(fileType, SIZE_READ_BY_FIRST_READ) / (double) totalFiles);
        String offsetDiffBetweenFirstAndSecondRead = String.format("%.3f", getMetricValue(fileType, OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ) / (double) totalFiles);

        if (NON_PARQUET.equals(fileType)) {
            sizeReadByFirstRead += "_" + String.format("%.3f", getMetricValue(fileType, FIRST_OFFSET_DIFF) / (double) totalFiles);
            offsetDiffBetweenFirstAndSecondRead += "_" + String.format("%.3f", getMetricValue(fileType, SECOND_OFFSET_DIFF) / (double) totalFiles);
        }

        metricBuilder.append("$").append(fileType)
                .append(":$FR=").append(sizeReadByFirstRead)
                .append("$SR=").append(offsetDiffBetweenFirstAndSecondRead)
                .append("$FL=").append(String.format("%.3f", getMetricValue(fileType, FILE_LENGTH) / (double) totalFiles))
                .append("$RL=").append(String.format("%.3f", getMetricValue(fileType, READ_LEN_REQUESTED) / (double) readCount));
    }

    @Override
    public String toString() {
        StringBuilder readFooterMetric = new StringBuilder();
        appendMetrics(readFooterMetric, PARQUET);
        appendMetrics(readFooterMetric, NON_PARQUET);
        return readFooterMetric.toString();
    }
}
