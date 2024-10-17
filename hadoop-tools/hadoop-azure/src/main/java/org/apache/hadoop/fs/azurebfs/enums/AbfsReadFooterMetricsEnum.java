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
