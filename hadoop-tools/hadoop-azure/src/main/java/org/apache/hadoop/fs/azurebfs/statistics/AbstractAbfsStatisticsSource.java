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

    public void incCounter(String name) {
        incCounter(name, 1);
    }

    public void incCounter(String name, long value) {
        ioStatistics.incrementCounter(name, value);
    }

    public Long lookupCounterValue(String name) {
        return ioStatistics.counters().getOrDefault(name, 0L);
    }

    public void setCounterValue(String name, long value) {
        ioStatistics.setCounter(name, value);
    }

    public Long lookupGaugeValue(String name) {
        return ioStatistics.gauges().getOrDefault(name, 0L);
    }

    public void updateGauge(String name, long value) {
        ioStatistics.setGauge(name, lookupGaugeValue(name) + value);
    }

    @Override
    public String toString() {
        return "AbstractAbfsStatisticsStore{" + ioStatistics + '}';
    }
}
