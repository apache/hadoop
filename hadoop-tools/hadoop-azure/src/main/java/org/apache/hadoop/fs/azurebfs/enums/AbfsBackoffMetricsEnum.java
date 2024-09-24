package org.apache.hadoop.fs.azurebfs.enums;

import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.BASE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;

public enum AbfsBackoffMetricsEnum {
    NUMBER_OF_IOPS_THROTTLED_REQUESTS("numberOfIOPSThrottledRequests", BASE, "Number of IOPS throttled requests"),
    NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS("numberOfBandwidthThrottledRequests", BASE, "Number of bandwidth throttled requests"),
    NUMBER_OF_OTHER_THROTTLED_REQUESTS("numberOfOtherThrottledRequests", BASE, "Number of other throttled requests"),
    NUMBER_OF_NETWORK_FAILED_REQUESTS("numberOfNetworkFailedRequests", BASE, "Number of network failed requests"),
    MAX_RETRY_COUNT("maxRetryCount", BASE, "Max retry count"),
    TOTAL_NUMBER_OF_REQUESTS("totalNumberOfRequests", BASE, "Total number of requests"),
    NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING("numberOfRequestsSucceededWithoutRetrying", BASE, "Number of requests succeeded without retrying"),
    NUMBER_OF_REQUESTS_FAILED("numberOfRequestsFailed", BASE, "Number of requests failed"),
    NUMBER_OF_REQUESTS_SUCCEEDED("numberOfRequestsSucceeded", RETRY,"Number of requests succeeded"),
    MIN_BACK_OFF("minBackOff", RETRY, "Minimum backoff"),
    MAX_BACK_OFF("maxBackOff", RETRY, "Maximum backoff"),
    TOTAL_BACK_OFF("totalBackoff", RETRY, "Total backoff"),
    TOTAL_REQUESTS("totalRequests", RETRY, "Total requests");

    private final String name;
    private final String type;
    private final String description;

    AbfsBackoffMetricsEnum(String name, String type, String description) {
        this.name = name;
        this.type = type;
        this.description = description;
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
}
