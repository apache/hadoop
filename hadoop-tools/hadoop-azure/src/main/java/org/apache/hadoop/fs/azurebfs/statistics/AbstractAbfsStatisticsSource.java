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

package org.apache.hadoop.fs.azurebfs.statistics;

import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

/**
 * Abstract class for Abfs statistics source.
 */
public abstract class AbstractAbfsStatisticsSource implements IOStatisticsSource {
    private IOStatisticsStore ioStatistics;

    /**
     * Default constructor.
     */
    protected AbstractAbfsStatisticsSource() {
    }

    /**
     * Returns the IOStatisticsStore instance.
     *
     * @return the IOStatisticsStore instance
     */
    @Override
    public IOStatisticsStore getIOStatistics() {
        return ioStatistics;
    }

    /**
     * Sets the IOStatisticsStore instance.
     *
     * @param ioStatistics the IOStatisticsStore instance to set
     */
    protected void setIOStatistics(final IOStatisticsStore ioStatistics) {
        this.ioStatistics = ioStatistics;
    }

    /**
     * Increments the counter value by 1 for the given name.
     *
     * @param name the name of the counter
     */
    public void incCounterValue(String name) {
        incCounterValue(name, 1);
    }

    /**
     * Increments the counter value by the specified value for the given name.
     *
     * @param name the name of the counter
     * @param value the value to increment by
     */
    public void incCounterValue(String name, long value) {
        ioStatistics.incrementCounter(name, value);
    }

    /**
     * Looks up the counter value for the given name.
     *
     * @param name the name of the counter
     * @return the counter value
     */
    public Long lookupCounterValue(String name) {
        return ioStatistics.counters().getOrDefault(name, 0L);
    }

    /**
     * Sets the counter value for the given name.
     *
     * @param name the name of the counter
     * @param value the value to set
     */
    public void setCounterValue(String name, long value) {
        ioStatistics.setCounter(name, value);
    }

    /**
     * Updates the counter value by adding the specified value to the current value for the given name.
     *
     * @param name the name of the counter
     * @param value the value to add
     */
    public void updateCounterValue(String name, long value) {
        ioStatistics.setCounter(name, lookupCounterValue(name) + value);
    }

    /**
     * Increments the gauge value by 1 for the given name.
     *
     * @param name the name of the gauge
     */
    public void incGaugeValue(String name) {
        incCounterValue(name, 1);
    }

    /**
     * Increments the gauge value by the specified value for the given name.
     *
     * @param name the name of the gauge
     * @param value the value to increment by
     */
    public void incGaugeValue(String name, long value) {
        ioStatistics.incrementGauge(name, value);
    }

    /**
     * Looks up the gauge value for the given name.
     *
     * @param name the name of the gauge
     * @return the gauge value
     */
    public Long lookupGaugeValue(String name) {
        return ioStatistics.gauges().getOrDefault(name, 0L);
    }

    /**
     * Updates the gauge value by adding the specified value to the current value for the given name.
     *
     * @param name the name of the gauge
     * @param value the value to add
     */
    public void updateGaugeValue(String name, long value) {
        ioStatistics.setGauge(name, lookupGaugeValue(name) + value);
    }

    /**
     * Sets the gauge value for the given name.
     *
     * @param name the name of the gauge
     * @param value the value to set
     */
    public void setGaugeValue(String name, long value) {
        ioStatistics.setGauge(name, value);
    }

    /**
     * Returns a string representation of the AbstractAbfsStatisticsSource.
     *
     * @return a string representation of the AbstractAbfsStatisticsSource
     */
    @Override
    public String toString() {
        return "AbstractAbfsStatisticsStore{" + ioStatistics + '}';
    }
}
