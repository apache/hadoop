package org.apache.hadoop.fs.azurebfs.enums;

import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.BASE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;

public enum AbfsBackoffMetricsEnum {
    NUMBER_OF_IOPS_THROTTLED_REQUESTS("numberOfIOPSThrottledRequests", BASE, "Number of IOPS throttled requests", "abc"),
    NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS("numberOfBandwidthThrottledRequests", BASE, "Number of bandwidth throttled requests", "def"),
    NUMBER_OF_OTHER_THROTTLED_REQUESTS("numberOfOtherThrottledRequests", BASE, "Number of other throttled requests", "def"),
    NUMBER_OF_NETWORK_FAILED_REQUESTS("numberOfNetworkFailedRequests", BASE, "Number of network failed requests", "def"),
    MAX_RETRY_COUNT("maxRetryCount", BASE, "Max retry count", "def"),
    TOTAL_NUMBER_OF_REQUESTS("totalNumberOfRequests", BASE, "Total number of requests", "def"),
    NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING("numberOfRequestsSucceededWithoutRetrying", BASE, "Number of requests succeeded without retrying", "def"),
    NUMBER_OF_REQUESTS_FAILED("numberOfRequestsFailed", BASE, "Number of requests failed", "def"),
    NUMBER_OF_REQUESTS_SUCCEEDED("numberOfRequestsSucceeded", RETRY,"Number of requests succeeded", "xyz"),
    MIN_BACK_OFF("minBackOff", RETRY, "Min back off", "def"),
    MAX_BACK_OFF("maxBackOff", RETRY, "Max back off", "def"),
    TOTAL_REQUESTS("totalRequests", RETRY, "Total requests", "def"),
    TOTAL_BACK_OFF("totalBackoff", RETRY, "Total back off", "def");

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
