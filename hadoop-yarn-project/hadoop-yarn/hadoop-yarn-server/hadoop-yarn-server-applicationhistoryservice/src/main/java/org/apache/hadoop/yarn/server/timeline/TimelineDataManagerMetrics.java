/*
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
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/** This class tracks metrics for the TimelineDataManager. */
@Metrics(about="Metrics for TimelineDataManager", context="yarn")
public class TimelineDataManagerMetrics {
  @Metric("getEntities calls")
  MutableCounterLong getEntitiesOps;

  @Metric("Entities returned via getEntities")
  MutableCounterLong getEntitiesTotal;

  @Metric("getEntities processing time")
  MutableRate getEntitiesTime;

  @Metric("getEntity calls")
  MutableCounterLong getEntityOps;

  @Metric("getEntity processing time")
  MutableRate getEntityTime;

  @Metric("getEvents calls")
  MutableCounterLong getEventsOps;

  @Metric("Events returned via getEvents")
  MutableCounterLong getEventsTotal;

  @Metric("getEvents processing time")
  MutableRate getEventsTime;

  @Metric("postEntities calls")
  MutableCounterLong postEntitiesOps;

  @Metric("Entities posted via postEntities")
  MutableCounterLong postEntitiesTotal;

  @Metric("postEntities processing time")
  MutableRate postEntitiesTime;

  @Metric("putDomain calls")
  MutableCounterLong putDomainOps;

  @Metric("putDomain processing time")
  MutableRate putDomainTime;

  @Metric("getDomain calls")
  MutableCounterLong getDomainOps;

  @Metric("getDomain processing time")
  MutableRate getDomainTime;

  @Metric("getDomains calls")
  MutableCounterLong getDomainsOps;

  @Metric("Domains returned via getDomains")
  MutableCounterLong getDomainsTotal;

  @Metric("getDomains processing time")
  MutableRate getDomainsTime;

  @Metric("Total calls")
  public long totalOps() {
    return getEntitiesOps.value() +
        getEntityOps.value() +
        getEventsOps.value() +
        postEntitiesOps.value() +
        putDomainOps.value() +
        getDomainOps.value() +
        getDomainsOps.value();
  }

  private static TimelineDataManagerMetrics instance = null;

  TimelineDataManagerMetrics() {
  }

  public static synchronized TimelineDataManagerMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(new TimelineDataManagerMetrics());
    }
    return instance;
  }

  public void incrGetEntitiesOps() {
    getEntitiesOps.incr();
  }

  public void incrGetEntitiesTotal(long delta) {
    getEntitiesTotal.incr(delta);
  }

  public void addGetEntitiesTime(long msec) {
    getEntitiesTime.add(msec);
  }

  public void incrGetEntityOps() {
    getEntityOps.incr();
  }

  public void addGetEntityTime(long msec) {
    getEntityTime.add(msec);
  }

  public void incrGetEventsOps() {
    getEventsOps.incr();
  }

  public void incrGetEventsTotal(long delta) {
    getEventsTotal.incr(delta);
  }

  public void addGetEventsTime(long msec) {
    getEventsTime.add(msec);
  }

  public void incrPostEntitiesOps() {
    postEntitiesOps.incr();
  }

  public void incrPostEntitiesTotal(long delta) {
    postEntitiesTotal.incr(delta);
  }

  public void addPostEntitiesTime(long msec) {
    postEntitiesTime.add(msec);
  }

  public void incrPutDomainOps() {
    putDomainOps.incr();
  }

  public void addPutDomainTime(long msec) {
    putDomainTime.add(msec);
  }

  public void incrGetDomainOps() {
    getDomainOps.incr();
  }

  public void addGetDomainTime(long msec) {
    getDomainTime.add(msec);
  }

  public void incrGetDomainsOps() {
    getDomainsOps.incr();
  }

  public void incrGetDomainsTotal(long delta) {
    getDomainsTotal.incr(delta);
  }

  public void addGetDomainsTime(long msec) {
    getDomainsTime.add(msec);
  }
}
