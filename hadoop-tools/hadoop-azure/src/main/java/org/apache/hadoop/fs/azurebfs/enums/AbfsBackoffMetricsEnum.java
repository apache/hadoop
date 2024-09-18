package org.apache.hadoop.fs.azurebfs.enums;

import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.BASE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;

public enum AbfsBackoffMetricsEnum {
    NUMBER_OF_REQUESTS_SUCCEEDED("numberOfRequestsSucceeded", RETRY,"Number of requests succeeded", "xyz"),
    NUMBER_OF_IOPS_THROTTLED_REQUESTS("numberOfIOPSThrottledRequests", BASE, "Number of IOPS throttled requests", "abc"),;

    private final String name;
    private final String type;
    private final String description;
    private final String code;

    AbfsBackoffMetricsEnum(String name, String type, String description, String code) {
        this.name = name;
        this.type = type;
        this.description = description;
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public String getCode() {
        return code;
    }
}
