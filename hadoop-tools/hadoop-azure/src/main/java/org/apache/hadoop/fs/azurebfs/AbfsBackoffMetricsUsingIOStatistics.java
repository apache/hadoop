package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum;
import org.apache.hadoop.fs.azurebfs.statistics.AbstractAbfsStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

public class AbfsBackoffMetricsUsingIOStatistics extends AbstractAbfsStatisticsSource {
    private final List<String> retryCountList = Arrays.asList("1", "2", "3", "4", "5_15", "15_25", "25AndAbove");

    public AbfsBackoffMetricsUsingIOStatistics() {
        IOStatisticsStore ioStatisticsStore = iostatisticsStore()
                .withCounters(getCountersName())
                .build();
        setIOStatistics(ioStatisticsStore);
    }

    private String[] getCountersName() {
        return Arrays.stream(AbfsBackoffMetricsEnum.values())
                .flatMap(backoffMetricsEnum -> {
                    if (RETRY.equals(backoffMetricsEnum.getType())) {
                        return retryCountList.stream()
                                .map(retryCount -> retryCount + ":" + backoffMetricsEnum.getName());
                    } else {
                        return Stream.of(backoffMetricsEnum.getName());
                    }
                })
                .toArray(String[]::new);
    }

    public void incrementCounter(AbfsBackoffMetricsEnum metric, String retryCount, long value) {
        incCounter(retryCount + ":" + metric.getName(), value);
    }

    public void incrementCounter(AbfsBackoffMetricsEnum metric, String retryCount) {
        incrementCounter(metric, retryCount, 1);
    }

    public void incrementCounter(AbfsBackoffMetricsEnum metric, long value) {
        incCounter(metric.getName(), value);
    }

    public void incrementCounter(AbfsBackoffMetricsEnum metric) {
        incrementCounter(metric, 1);
    }

    public Long lookupCounterValue(AbfsBackoffMetricsEnum metric, String retryCount) {
        return lookupCounterValue(retryCount + ":" + metric.getName());
    }

    public Long lookupCounterValue(AbfsBackoffMetricsEnum metric) {
        return lookupCounterValue(metric.getName());
    }
}
