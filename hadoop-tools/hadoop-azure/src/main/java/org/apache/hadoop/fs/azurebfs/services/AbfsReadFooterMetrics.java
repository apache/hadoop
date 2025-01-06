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
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.enums.AbfsReadFooterMetricsEnum;
import org.apache.hadoop.fs.azurebfs.enums.FileType;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_UNDERSCORE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.CHAR_DOLLAR;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.DOUBLE_PRECISION_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.FIRST_READ;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SECOND_READ;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.FILE_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.READ_LENGTH;
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
    private static final Logger LOG = LoggerFactory.getLogger(AbfsReadFooterMetrics.class);
    private static final String FOOTER_LENGTH = "20";
    private static final List<FileType> FILE_TYPE_LIST =
            Arrays.asList(FileType.values());
    private final Map<String, FileTypeMetrics> fileTypeMetricsMap =
            new ConcurrentHashMap<>();

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

        /**
         * Constructor to initialize the file type metrics.
         */
        private FileTypeMetrics() {
            collectMetrics = new AtomicBoolean(false);
            collectMetricsForNextRead = new AtomicBoolean(false);
            collectLenMetrics = new AtomicBoolean(false);
            readCount = new AtomicLong(0);
            offsetOfFirstRead = new AtomicLong(0);
        }

        /**
         * Updates the file type based on the metrics collected.
         */
        private void updateFileType() {
            if (fileType == null) {
                fileType = collectMetrics.get() && readCount.get() >= 2
                        && haveEqualValues(sizeReadByFirstRead)
                        && haveEqualValues(offsetDiffBetweenFirstAndSecondRead) ? PARQUET : NON_PARQUET;
            }
        }

        /**
         * Checks if the given value has equal parts.
         *
         * @param value the value to check
         * @return true if the value has equal parts, false otherwise
         */
        private boolean haveEqualValues(String value) {
            String[] parts = value.split("_");
            return parts.length == 2
                    && parts[0].equals(parts[1]);
        }

        /**
         * Increments the read count.
         */
        private void incrementReadCount() {
            readCount.incrementAndGet();
        }

        /**
         * Returns the read count.
         *
         * @return the read count
         */
        private long getReadCount() {
            return readCount.get();
        }

        /**
         * Sets the collect metrics flag.
         *
         * @param collect the value to set
         */
        private void setCollectMetrics(boolean collect) {
            collectMetrics.set(collect);
        }

        /**
         * Returns the collect metrics flag.
         *
         * @return the collect metrics flag
         */
        private boolean getCollectMetrics() {
            return collectMetrics.get();
        }

        /**
         * Sets the collect metrics for the next read flag.
         *
         * @param collect the value to set
         */
        private void setCollectMetricsForNextRead(boolean collect) {
            collectMetricsForNextRead.set(collect);
        }

        /**
         * Returns the collect metrics for the next read flag.
         *
         * @return the collect metrics for the next read flag
         */
        private boolean getCollectMetricsForNextRead() {
            return collectMetricsForNextRead.get();
        }

        /**
         * Returns the collect length metrics flag.
         *
         * @return the collect length metrics flag
         */
        private boolean getCollectLenMetrics() {
            return collectLenMetrics.get();
        }

        /**
         * Sets the collect length metrics flag.
         *
         * @param collect the value to set
         */
        private void setCollectLenMetrics(boolean collect) {
            collectLenMetrics.set(collect);
        }

        /**
         * Sets the offset of the first read.
         *
         * @param offset the value to set
         */
        private void setOffsetOfFirstRead(long offset) {
            offsetOfFirstRead.set(offset);
        }

        /**
         * Returns the offset of the first read.
         *
         * @return the offset of the first read
         */
        private long getOffsetOfFirstRead() {
            return offsetOfFirstRead.get();
        }

        /**
         * Sets the size read by the first read.
         *
         * @param size the value to set
         */
        private void setSizeReadByFirstRead(String size) {
            sizeReadByFirstRead = size;
        }

        /**
         * Returns the size read by the first read.
         *
         * @return the size read by the first read
         */
        private String getSizeReadByFirstRead() {
            return sizeReadByFirstRead;
        }

        /**
         * Sets the offset difference between the first and second read.
         *
         * @param offsetDiff the value to set
         */
        private void setOffsetDiffBetweenFirstAndSecondRead(String offsetDiff) {
            offsetDiffBetweenFirstAndSecondRead = offsetDiff;
        }

        /**
         * Returns the offset difference between the first and second read.
         *
         * @return the offset difference between the first and second read
         */
        private String getOffsetDiffBetweenFirstAndSecondRead() {
            return offsetDiffBetweenFirstAndSecondRead;
        }

        /**
         * Returns the file type.
         *
         * @return the file type
         */
        private FileType getFileType() {
            return fileType;
        }
    }

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

    /**
     * Returns the metric names for a specific statistic type.
     *
     * @param type the statistic type
     * @return the metric names
     */
    private String[] getMetricNames(StatisticTypeEnum type) {
        return Arrays.stream(AbfsReadFooterMetricsEnum.values())
                .filter(readFooterMetricsEnum -> readFooterMetricsEnum.getStatisticType().equals(type))
                .flatMap(readFooterMetricsEnum ->
                        FILE.equals(readFooterMetricsEnum.getType())
                                ? FILE_TYPE_LIST.stream().map(fileType ->
                                getMetricName(fileType, readFooterMetricsEnum))
                                : Stream.of(readFooterMetricsEnum.getName()))
                .toArray(String[]::new);
    }

    /**
     * Returns the metric name for a specific file type and metric.
     *
     * @param fileType the type of the file
     * @param readFooterMetricsEnum the metric to get the name for
     * @return the metric name
     */
    private String getMetricName(FileType fileType,
                                 AbfsReadFooterMetricsEnum readFooterMetricsEnum) {
        if (fileType == null || readFooterMetricsEnum == null) {
            LOG.error("File type or ABFS read footer metrics should not be null");
            return EMPTY_STRING;
        }
        return fileType + COLON + readFooterMetricsEnum.getName();
    }

    /**
     * Looks up the counter value for a specific metric.
     *
     * @param fileType the type of the file
     * @param abfsReadFooterMetricsEnum the metric to look up
     * @return the counter value
     */
    private long getCounterMetricValue(FileType fileType,
                                       AbfsReadFooterMetricsEnum abfsReadFooterMetricsEnum) {
        return lookupCounterValue(getMetricName(fileType, abfsReadFooterMetricsEnum));
    }

    /**
     * Looks up the mean statistic value for a specific metric.
     *
     * @param fileType the type of the file
     * @param abfsReadFooterMetricsEnum the metric to look up
     * @return the mean statistic value
     */
    private String getMeanMetricValue(FileType fileType,
                                      AbfsReadFooterMetricsEnum abfsReadFooterMetricsEnum) {
        return format(DOUBLE_PRECISION_FORMAT,
                lookupMeanStatistic(getMetricName(fileType, abfsReadFooterMetricsEnum)));
    }

    /**
     * Increments the value of a specific metric.
     *
     * @param fileType the type of the file
     * @param abfsReadFooterMetricsEnum the metric to increment
     */
    public void incrementMetricValue(FileType fileType,
                                     AbfsReadFooterMetricsEnum abfsReadFooterMetricsEnum) {
        incCounterValue(getMetricName(fileType, abfsReadFooterMetricsEnum));
    }

    /**
     * Adds a mean statistic value for a specific metric.
     *
     * @param fileType the type of the file
     * @param abfsReadFooterMetricsEnum the metric to update
     * @param value the new value of the metric
     */
    public void addMeanMetricValue(FileType fileType,
                                   AbfsReadFooterMetricsEnum abfsReadFooterMetricsEnum,
                                   long value) {
        addMeanStatistic(getMetricName(fileType, abfsReadFooterMetricsEnum), value);
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
    public void updateReadMetrics(final String filePathIdentifier,
                                  final int len,
                                  final long contentLength,
                                  final long nextReadPos) {
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
    private void updateMetrics(FileTypeMetrics fileTypeMetrics,
                               int len,
                               long contentLength,
                               long nextReadPos) {
        fileTypeMetrics.incrementReadCount();

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
    private void handleFirstRead(FileTypeMetrics fileTypeMetrics,
                                 long nextReadPos,
                                 int len,
                                 long contentLength) {
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
    private void handleSecondRead(FileTypeMetrics fileTypeMetrics,
                                  long nextReadPos,
                                  int len,
                                  long contentLength) {
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
    private void handleFurtherRead(FileTypeMetrics fileTypeMetrics, int len) {
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
    private void updateMetricsData(FileTypeMetrics fileTypeMetrics,
                                                int len,
                                                long contentLength) {
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

    /**
     * Appends the metrics for a specific file type to the given metric builder.
     *
     * @param metricBuilder the metric builder to append the metrics to
     * @param fileType the file type to append the metrics for
     */
    private void appendMetrics(StringBuilder metricBuilder, FileType fileType) {
        long totalFiles = getCounterMetricValue(fileType, TOTAL_FILES);
        if (totalFiles <= 0) {
            return;
        }

        String sizeReadByFirstRead = getMeanMetricValue(fileType, AVG_SIZE_READ_BY_FIRST_READ);
        String offsetDiffBetweenFirstAndSecondRead = getMeanMetricValue(fileType, AVG_OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ);

        if (NON_PARQUET.equals(fileType)) {
            sizeReadByFirstRead += CHAR_UNDERSCORE + getMeanMetricValue(fileType, AVG_FIRST_OFFSET_DIFF);
            offsetDiffBetweenFirstAndSecondRead += CHAR_UNDERSCORE + getMeanMetricValue(fileType, AVG_SECOND_OFFSET_DIFF);
        }

        metricBuilder.append(CHAR_DOLLAR)
                .append(fileType)
                .append(FIRST_READ)
                .append(sizeReadByFirstRead)
                .append(SECOND_READ)
                .append(offsetDiffBetweenFirstAndSecondRead)
                .append(FILE_LENGTH)
                .append(getMeanMetricValue(fileType, AVG_FILE_LENGTH))
                .append(READ_LENGTH)
                .append(getMeanMetricValue(fileType, AVG_READ_LEN_REQUESTED));
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
