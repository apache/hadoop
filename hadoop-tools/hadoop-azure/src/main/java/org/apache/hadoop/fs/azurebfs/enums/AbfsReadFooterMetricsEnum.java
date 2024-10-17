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

    AbfsReadFooterMetricsEnum(String name, String description, String type, StatisticTypeEnum statisticType) {
        this.name = name;
        this.description = description;
        this.type = type;
        this.statisticType = statisticType;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    public StatisticTypeEnum getStatisticType() {
        return statisticType;
    }
}
