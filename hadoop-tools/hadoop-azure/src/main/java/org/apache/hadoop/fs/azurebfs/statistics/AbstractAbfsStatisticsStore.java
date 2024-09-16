package org.apache.hadoop.fs.azurebfs.statistics;

import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class AbstractAbfsStatisticsStore implements IOStatisticsSource {
    private static final String BASE_KEY = "BASE";
    private final Map<String, IOStatisticsStore> ioStatisticsMap = new ConcurrentHashMap<>();

    protected AbstractAbfsStatisticsStore() {
    }

    @Override
    public IOStatisticsStore getIOStatistics() {
        return getIOStatistics(BASE_KEY);
    }

    protected IOStatisticsStore getIOStatistics(String key) {
        return ioStatisticsMap.getOrDefault(key, null);
    }

    protected Map<String, IOStatisticsStore> getIOStatisticsMap() {
        return ioStatisticsMap;
    }

    protected void setIOStatistics(final IOStatisticsStore statistics) {
        setIOStatistics(BASE_KEY, statistics);
    }

    protected void setIOStatistics(String key, final IOStatisticsStore statistics) {
        ioStatisticsMap.put(key, statistics);
    }

    protected long incCounter(String name, long value) {
        return incCounter(BASE_KEY, name, value);
    }

    protected long incCounter(String key, String name) {
        return incCounter(key, name, 1);
    }

    protected long incCounter(String key, String name, long value) {
        return ioStatisticsMap.getOrDefault(key, null) == null ? 0L : ioStatisticsMap.get(key).incrementCounter(name, value);
    }

    protected String statisticsToString(String key) {
        IOStatisticsStore ioStatistics = ioStatisticsMap.getOrDefault(key, null);

        if (ioStatistics == null || ioStatistics.counters().isEmpty()) { return null; }

        return ioStatistics.counters().entrySet().stream()
                .map(entry -> "\"" + entry.getKey() + "\": " + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
