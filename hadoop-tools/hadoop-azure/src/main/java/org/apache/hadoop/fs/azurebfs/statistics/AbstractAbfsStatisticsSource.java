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

    public void incCounterValue(String name) {
        incCounterValue(name, 1);
    }

    public void incCounterValue(String name, long value) {
        ioStatistics.incrementCounter(name, value);
    }

    public Long lookupCounterValue(String name) {
        return ioStatistics.counters().getOrDefault(name, 0L);
    }

    public void setCounterValue(String name, long value) {
        ioStatistics.setCounter(name, value);
    }

    public void updateCounterValue(String name, long value) {
        ioStatistics.setCounter(name, lookupCounterValue(name) + value);
    }

    public void incGaugeValue(String name) {
        incCounterValue(name, 1);
    }

    public void incGaugeValue(String name, long value) {
        ioStatistics.incrementGauge(name, value);
    }

    public Long lookupGaugeValue(String name) {
        return ioStatistics.gauges().getOrDefault(name, 0L);
    }

    public void updateGaugeValue(String name, long value) {
        ioStatistics.setGauge(name, lookupGaugeValue(name) + value);
    }

    public void setGaugeValue(String name, long value) {
        ioStatistics.setGauge(name, value);
    }

    @Override
    public String toString() {
        return "AbstractAbfsStatisticsStore{" + ioStatistics + '}';
    }
}
