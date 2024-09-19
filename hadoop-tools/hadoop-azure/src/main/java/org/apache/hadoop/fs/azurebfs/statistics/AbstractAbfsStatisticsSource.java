package org.apache.hadoop.fs.azurebfs.statistics;

import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

public abstract class AbstractAbfsStatisticsSource implements IOStatisticsSource {
    private IOStatisticsStore ioStatistics;

    protected AbstractAbfsStatisticsSource() {
    }

    @Override
    public IOStatisticsStore getIOStatistics() {
        return ioStatistics;
    }

    protected void setIOStatistics(final IOStatisticsStore ioStatistics) {
        this.ioStatistics = ioStatistics;
    }

    public long incCounter(String name) {
        return incCounter(name, 1);
    }

    public long incCounter(String name, long value) {
        return ioStatistics.incrementCounter(name, value);
    }

    public Long lookupCounterValue(String name) {
        return ioStatistics.counters().get(name);
    }

    public void setCounterValue(String name, long value) {
        ioStatistics.setCounter(name, value);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(
                "AbstractAbfsStatisticsStore{");
        sb.append(ioStatistics);
        sb.append('}');
        return sb.toString();
    }
}
