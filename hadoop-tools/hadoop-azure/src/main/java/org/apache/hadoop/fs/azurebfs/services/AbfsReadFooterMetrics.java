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
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.DOUBLE_PRECISION_FORMAT;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.TOTAL_FILES;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_FILE_LENGTH;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_SIZE_READ_BY_FIRST_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_READ_LEN_REQUESTED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_FIRST_OFFSET_DIFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum.AVG_SECOND_OFFSET_DIFF;
import static org.apache.hadoop.fs.azurebfs.enums.FileType.PARQUET;
import static org.apache.hadoop.fs.azurebfs.enums.FileType.NON_PARQUET;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_MEAN;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.util.StringUtils.format;

/**
 * This class is responsible for tracking and updating metrics related to reading footers in files.
 */
public class AbfsReadFooterMetrics extends AbstractAbfsStatisticsSource {
    private static final String FOOTER_LENGTH = "20";
    private static final List<FileType> FILE_TYPE_LIST = Arrays.asList(PARQUET, NON_PARQUET);

    /**
     * Inner class to handle file type checks.
     */
    private static final class FileTypeMetrics {
        private final AtomicBoolean collectMetrics;
        private final AtomicBoolean collectMetricsForNextRead;
        private final AtomicBoolean collectLenMetrics;
        private final AtomicLong readCount;
        private final AtomicLong offsetOfFirstRead;
        private FileType fileType = null;
        private String sizeReadByFirstRead;
        private String offsetDiffBetweenFirstAndSecondRead;

        private FileTypeMetrics() {
            collectMetrics = new AtomicBoolean(false);
            collectMetricsForNextRead = new AtomicBoolean(false);
            collectLenMetrics = new AtomicBoolean(false);
            readCount = new AtomicLong(0);
            offsetOfFirstRead = new AtomicLong(0);
        }

        private void updateFileType() {
            if (fileType == null) {
                fileType = collectMetrics.get() && readCount.get() >= 2
                        && haveEqualValues(sizeReadByFirstRead)
                        && haveEqualValues(offsetDiffBetweenFirstAndSecondRead) ? PARQUET : NON_PARQUET;
            }
        }

        private boolean haveEqualValues(String value) {
            String[] parts = value.split("_");
            return parts.length == 2
                    && parts[0].equals(parts[1]);
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

    private final Map<String, FileTypeMetrics> fileTypeMetricsMap = new HashMap<>();

    /**
     * Constructor to initialize the IOStatisticsStore with counters and mean statistics.
     */
    public AbfsReadFooterMetrics() {
        IOStatisticsStore ioStatisticsStore = iostatisticsStore()
                .withCounters(getMetricNames(TYPE_COUNTER))
                .withMeanStatistics(getMetricNames(TYPE_MEAN))
                .build();
        setIOStatistics(ioStatisticsStore);
    }

    private String[] getMetricNames(StatisticTypeEnum type) {
        return Arrays.stream(AbfsReadFooterMetricsEnum.values())
                .filter(readFooterMetricsEnum -> readFooterMetricsEnum.getStatisticType().equals(type))
                .flatMap(readFooterMetricsEnum ->
                        FILE.equals(readFooterMetricsEnum.getType())
                                ? FILE_TYPE_LIST.stream().map(fileType -> fileType + COLON + readFooterMetricsEnum.getName())
                                : Stream.of(readFooterMetricsEnum.getName()))
                .toArray(String[]::new);
    }

    private long getCounterMetricValue(FileType fileType, AbfsReadFooterMetricsEnum metric) {
        return lookupCounterValue(fileType + COLON + metric.getName());
    }

    private String getMeanMetricValue(FileType fileType, AbfsReadFooterMetricsEnum metric) {
        return format(DOUBLE_PRECISION_FORMAT, lookupMeanStatistic(fileType + COLON + metric.getName()));
    }

    /**
     * Increments the value of a specific metric.
     *
     * @param fileType the type of the file
     * @param metricName the metric to increment
     */
    public void incrementMetricValue(FileType fileType, AbfsReadFooterMetricsEnum metricName) {
        incCounterValue(fileType + COLON + metricName.getName());
    }

    /**
     * Adds a mean statistic value for a specific metric.
     *
     * @param fileType the type of the file
     * @param metricName the metric to update
     * @param value the new value of the metric
     */
    public void addMeanMetricValue(FileType fileType, AbfsReadFooterMetricsEnum metricName, long value) {
        addMeanStatistic(fileType + COLON + metricName.getName(), value);
    }

    /**
     * Returns the total number of files.
     *
     * @return the total number of files
     */
    public Long getTotalFiles() {
        return getCounterMetricValue(PARQUET, TOTAL_FILES) + getCounterMetricValue(NON_PARQUET, TOTAL_FILES);
    }

    /**
     * Updates the map with a new file path identifier.
     *
     * @param filePathIdentifier the file path identifier
     */
    public void updateMap(String filePathIdentifier) {
        fileTypeMetricsMap.computeIfAbsent(filePathIdentifier, key -> new FileTypeMetrics());
    }

    /**
     * Checks and updates the metrics for a given file read.
     *
     * @param filePathIdentifier the file path identifier
     * @param len the length of the read
     * @param contentLength the total content length of the file
     * @param nextReadPos the position of the next read
     */
    public void updateReadMetrics(final String filePathIdentifier, final int len, final long contentLength, final long nextReadPos) {
        FileTypeMetrics fileTypeMetrics = fileTypeMetricsMap.computeIfAbsent(filePathIdentifier, key -> new FileTypeMetrics());
        if (fileTypeMetrics.getReadCount() == 0 || (fileTypeMetrics.getReadCount() >= 1 && fileTypeMetrics.getCollectMetrics())) {
            updateMetrics(fileTypeMetrics, len, contentLength, nextReadPos);
        }
    }

    /**
     * Updates metrics for a specific file identified by filePathIdentifier.
     *
     * @param fileTypeMetrics    File metadata to know file type.
     * @param len                The length of the read operation.
     * @param contentLength      The total content length of the file.
     * @param nextReadPos        The position of the next read operation.
     */
    private void updateMetrics(FileTypeMetrics fileTypeMetrics, int len, long contentLength, long nextReadPos) {
        synchronized (this) {
            fileTypeMetrics.incrementReadCount();
        }

        long readCount = fileTypeMetrics.getReadCount();

        if (readCount == 1) {
            handleFirstRead(fileTypeMetrics, nextReadPos, len, contentLength);
        } else if (readCount == 2) {
            handleSecondRead(fileTypeMetrics, nextReadPos, len, contentLength);
        } else {
            handleFurtherRead(fileTypeMetrics, len);
        }
    }

    /**
     * Handles the first read operation by checking if the current read position is near the end of the file.
     * If it is, updates the {@link FileTypeMetrics} object to enable metrics collection and records the first read's
     * offset and size.
     *
     * @param fileTypeMetrics The {@link FileTypeMetrics} object to update with metrics and read details.
     * @param nextReadPos The position where the next read will start.
     * @param len The length of the current read operation.
     * @param contentLength The total length of the file content.
     */
    private void handleFirstRead(FileTypeMetrics fileTypeMetrics, long nextReadPos, int len, long contentLength) {
        if (nextReadPos >= contentLength - (long) Integer.parseInt(FOOTER_LENGTH) * ONE_KB) {
            fileTypeMetrics.setCollectMetrics(true);
            fileTypeMetrics.setCollectMetricsForNextRead(true);
            fileTypeMetrics.setOffsetOfFirstRead(nextReadPos);
            fileTypeMetrics.setSizeReadByFirstRead(len + "_" + Math.abs(contentLength - nextReadPos));
        }
    }

    /**
     * Handles the second read operation by checking if metrics collection is enabled for the next read.
     * If it is, calculates the offset difference between the first and second reads, updates the {@link FileTypeMetrics}
     * object with this information, and sets the file type. Then, updates the metrics data.
     *
     * @param fileTypeMetrics The {@link FileTypeMetrics} object to update with metrics and read details.
     * @param nextReadPos The position where the next read will start.
     * @param len The length of the current read operation.
     * @param contentLength The total length of the file content.
     */
    private void handleSecondRead(FileTypeMetrics fileTypeMetrics, long nextReadPos, int len, long contentLength) {
        if (fileTypeMetrics.getCollectMetricsForNextRead()) {
            long offsetDiff = Math.abs(nextReadPos - fileTypeMetrics.getOffsetOfFirstRead());
            fileTypeMetrics.setOffsetDiffBetweenFirstAndSecondRead(len + "_" + offsetDiff);
            fileTypeMetrics.setCollectLenMetrics(true);
            fileTypeMetrics.updateFileType();
            updateMetricsData(fileTypeMetrics, len, contentLength);
        }
    }

    /**
     * Handles further read operations beyond the second read. If metrics collection is enabled and the file type is set,
     * updates the read length requested and increments the read count for the specific file type.
     *
     * @param fileTypeMetrics The {@link FileTypeMetrics} object containing metrics and read details.
     * @param len The length of the current read operation.
     */
    private synchronized void handleFurtherRead(FileTypeMetrics fileTypeMetrics, int len) {
        if (fileTypeMetrics.getCollectLenMetrics() && fileTypeMetrics.getFileType() != null) {
            FileType fileType = fileTypeMetrics.getFileType();
            addMeanMetricValue(fileType, AVG_READ_LEN_REQUESTED, len);
        }
    }

    /**
     * Updates the metrics data for a specific file identified by the {@link FileTypeMetrics} object.
     * This method calculates and updates various metrics such as read length requested, file length,
     * size read by the first read, and offset differences between reads.
     *
     * @param fileTypeMetrics The {@link FileTypeMetrics} object containing metrics and read details.
     * @param len The length of the current read operation.
     * @param contentLength The total length of the file content.
     */
    private synchronized void updateMetricsData(FileTypeMetrics fileTypeMetrics, int len, long contentLength) {
        long sizeReadByFirstRead = Long.parseLong(fileTypeMetrics.getSizeReadByFirstRead().split("_")[0]);
        long firstOffsetDiff = Long.parseLong(fileTypeMetrics.getSizeReadByFirstRead().split("_")[1]);
        long secondOffsetDiff = Long.parseLong(fileTypeMetrics.getOffsetDiffBetweenFirstAndSecondRead().split("_")[1]);
        FileType fileType = fileTypeMetrics.getFileType();

        addMeanMetricValue(fileType, AVG_READ_LEN_REQUESTED, len);
        addMeanMetricValue(fileType, AVG_READ_LEN_REQUESTED, sizeReadByFirstRead);
        addMeanMetricValue(fileType, AVG_FILE_LENGTH, contentLength);
        addMeanMetricValue(fileType, AVG_SIZE_READ_BY_FIRST_READ, sizeReadByFirstRead);
        addMeanMetricValue(fileType, AVG_OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ, len);
        addMeanMetricValue(fileType, AVG_FIRST_OFFSET_DIFF, firstOffsetDiff);
        addMeanMetricValue(fileType, AVG_SECOND_OFFSET_DIFF, secondOffsetDiff);
        incrementMetricValue(fileType, TOTAL_FILES);
    }

    private void appendMetrics(StringBuilder metricBuilder, FileType fileType) {
        long totalFiles = getCounterMetricValue(fileType, TOTAL_FILES);
        if (totalFiles <= 0) {
            return;
        }

        String sizeReadByFirstRead = getMeanMetricValue(fileType, AVG_SIZE_READ_BY_FIRST_READ);
        String offsetDiffBetweenFirstAndSecondRead = getMeanMetricValue(fileType, AVG_OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ);

        if (NON_PARQUET.equals(fileType)) {
            sizeReadByFirstRead += "_" + getMeanMetricValue(fileType, AVG_FIRST_OFFSET_DIFF);
            offsetDiffBetweenFirstAndSecondRead += "_" + getMeanMetricValue(fileType, AVG_SECOND_OFFSET_DIFF);
        }

        metricBuilder.append("$").append(fileType)
                .append(":$FR=").append(sizeReadByFirstRead)
                .append("$SR=").append(offsetDiffBetweenFirstAndSecondRead)
                .append("$FL=").append(getMeanMetricValue(fileType, AVG_FILE_LENGTH))
                .append("$RL=").append(getMeanMetricValue(fileType, AVG_READ_LEN_REQUESTED));
    }

    /**
     * Returns the read footer metrics for all file types.
     *
     * @return the read footer metrics as a string
     */
    @Override
    public String toString() {
        StringBuilder readFooterMetric = new StringBuilder();
        appendMetrics(readFooterMetric, PARQUET);
        appendMetrics(readFooterMetric, NON_PARQUET);
        return readFooterMetric.toString();
    }
}
