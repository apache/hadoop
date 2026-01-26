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

import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.BASE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_GAUGE;

/**
 * Enum representing various ABFS backoff metrics
 */
public enum AbfsBackoffMetricsEnum {
    NUMBER_OF_IOPS_THROTTLED_REQUESTS("numberOfIOPSThrottledRequests",
            "Number of IOPS throttled requests", BASE, TYPE_COUNTER),
    NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS("numberOfBandwidthThrottledRequests",
            "Number of bandwidth throttled requests", BASE, TYPE_COUNTER),
    NUMBER_OF_OTHER_THROTTLED_REQUESTS("numberOfOtherThrottledRequests",
            "Number of other throttled requests", BASE, TYPE_COUNTER),
    NUMBER_OF_NETWORK_FAILED_REQUESTS("numberOfNetworkFailedRequests",
            "Number of network failed requests", BASE, TYPE_COUNTER),
    MAX_RETRY_COUNT("maxRetryCount", "Max retry count", BASE, TYPE_COUNTER),
    TOTAL_NUMBER_OF_REQUESTS("totalNumberOfRequests",
            "Total number of requests", BASE, TYPE_COUNTER),
    NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING("numberOfRequestsSucceededWithoutRetrying",
            "Number of requests succeeded without retrying", BASE, TYPE_COUNTER),
    NUMBER_OF_REQUESTS_FAILED("numberOfRequestsFailed",
            "Number of requests failed", BASE, TYPE_COUNTER),
    NUMBER_OF_REQUESTS_SUCCEEDED("numberOfRequestsSucceeded",
            "Number of requests succeeded", RETRY, TYPE_COUNTER),
    MIN_BACK_OFF("minBackOff", "Minimum backoff", RETRY, TYPE_GAUGE),
    MAX_BACK_OFF("maxBackOff", "Maximum backoff", RETRY, TYPE_GAUGE),
    TOTAL_BACK_OFF("totalBackoff", "Total backoff", RETRY, TYPE_GAUGE),
    TOTAL_REQUESTS("totalRequests", "Total requests", RETRY, TYPE_COUNTER);

    private final String name;
    private final String description;
    private final String type;
    private final StatisticTypeEnum statisticType;

    /**
     * Constructor for AbfsBackoffMetricsEnum.
     *
     * @param name the name of the metric
     * @param description the description of the metric
     * @param type the type of the metric (BASE or RETRY)
     * @param statisticType the statistic type of the metric (counter or gauge)
     */
    AbfsBackoffMetricsEnum(String name,
                           String description,
                           String type,
                           StatisticTypeEnum statisticType) {
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
