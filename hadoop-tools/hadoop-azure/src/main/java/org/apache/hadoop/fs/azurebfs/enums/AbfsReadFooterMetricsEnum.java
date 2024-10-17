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

import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.COUNTER;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.GAUGE;

public enum AbfsReadFooterMetricsEnum {
    TOTAL_FILES("totalFiles", "Total files in a file system", COUNTER),
    FILE_LENGTH("fileLength", "File length", GAUGE),
    SIZE_READ_BY_FIRST_READ("sizeReadByFirstRead", "Size read by first read", GAUGE),
    OFFSET_DIFF_BETWEEN_FIRST_AND_SECOND_READ("offsetDiffBetweenFirstAndSecondRead", "Offset difference between first and second read", GAUGE),
    READ_LEN_REQUESTED("readLenRequested", "Read length requested", GAUGE),
    READ_COUNT("readCount", "Read count", COUNTER),
    FIRST_OFFSET_DIFF("firstOffsetDiff", "First offset difference", GAUGE),
    SECOND_OFFSET_DIFF("secondOffsetDiff", "Second offset difference", GAUGE);

    private final String name;
    private final String description;
    private final String type;

    AbfsReadFooterMetricsEnum(String name, String description, String type) {
        this.name = name;
        this.description = description;
        this.type = type;
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
}
