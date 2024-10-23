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

package org.apache.hadoop.fs.azurebfs.enums;

import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_GAUGE;

/**
 * Enum representing various ABFS read footer metrics.
 */
public enum AbfsReadFooterMetricsEnum {
    TOTAL_FILES("totalFiles", "Total files in a file system", FILE,  TYPE_COUNTER),
    FILE_LENGTH("fileLength", "File length", FILE, TYPE_GAUGE),
    SIZE_READ_BY_FIRST_READ("sizeReadByFirstRead", "Size read by first read", FILE, TYPE_GAUGE),
    OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ("offsetDiffBetweenFirstAndSecondRead", "Offset difference between first and second read", FILE, TYPE_GAUGE),
    READ_LEN_REQUESTED("readLenRequested", "Read length requested", FILE, TYPE_GAUGE),
    READ_COUNT("readCount", "Read count", FILE, TYPE_COUNTER),
    FIRST_OFFSET_DIFF("firstOffsetDiff", "First offset difference", FILE, TYPE_GAUGE),
    SECOND_OFFSET_DIFF("secondOffsetDiff", "Second offset difference", FILE, TYPE_GAUGE);

    private final String name;
    private final String description;
    private final String type;
    private final StatisticTypeEnum statisticType;

    /**
     * Constructor for AbfsReadFooterMetricsEnum.
     *
     * @param name the name of the metric
     * @param description the description of the metric
     * @param type the type of the metric (FILE)
     * @param statisticType the statistic type of the metric (counter or gauge)
     */
    AbfsReadFooterMetricsEnum(String name, String description, String type, StatisticTypeEnum statisticType) {
        this.name = name;
        this.description = description;
        this.type = type;
        this.statisticType = statisticType;
    }

    /**
     * Gets the name of the metric.
     *
     * @return the name of the metric
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the description of the metric.
     *
     * @return the description of the metric
     */
    public String getDescription() {
        return description;
    }

    /**
     * Gets the type of the metric.
     *
     * @return the type of the metric
     */
    public String getType() {
        return type;
    }

    /**
     * Gets the statistic type of the metric.
     *
     * @return the statistic type of the metric
     */
    public StatisticTypeEnum getStatisticType() {
        return statisticType;
    }
}
