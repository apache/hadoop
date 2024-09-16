package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.statistics.AbstractAbfsStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

public class InputStreamStatistics extends AbstractAbfsStatisticsStore {
    public InputStreamStatistics() {
        IOStatisticsStore st = iostatisticsStore()
                .withCounters("TEST1", "TEST2")
                .build();
        setIOStatistics(st);

        IOStatisticsStore retrySt = iostatisticsStore()
                .withCounters("TEST3", "TEST4")
                .build();
        setIOStatistics("RETRY_ONE", retrySt);
    }

    public InputStreamStatistics(Map<String, List<String>> map) {
        for(Map.Entry<String, List<String>> entry : map.entrySet()) {
            IOStatisticsStore st = iostatisticsStore()
                    .withCounters(entry.getValue().toArray(new String[0]))
                    .build();
            setIOStatistics(entry.getKey(), st);
        }
    }

    public IOStatisticsStore localIOStatistics() {
        return InputStreamStatistics.super.getIOStatistics();
    }

    public IOStatisticsStore localIOStatistics(String key) {
        return InputStreamStatistics.super.getIOStatistics(key);
    }

    public Map<String, IOStatisticsStore> localIOStatisticsMap() {
        return InputStreamStatistics.super.getIOStatisticsMap();
    }

    public long increment(String name) {
        return increment(name, 1);
    }

    public long increment(String name, long value) {
        return incCounter(name, value);
    }

    public long increment(String key, String name) {
        return incCounter(key, name);
    }

    public long increment(String key, String name, long value) {
        return incCounter(key, name, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        String result = localIOStatisticsMap().keySet().stream()
                .map(key -> "\"" + key + "\": " + statisticsToString(key))
                .collect(Collectors.joining(", "));

        sb.append(result);
        sb.append('}');
        return sb.toString();
    }
}
