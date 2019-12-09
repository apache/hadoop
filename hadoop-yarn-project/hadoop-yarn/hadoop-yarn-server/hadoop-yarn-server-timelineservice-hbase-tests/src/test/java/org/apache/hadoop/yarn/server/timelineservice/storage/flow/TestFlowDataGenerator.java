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

package org.apache.hadoop.yarn.server.timelineservice.storage.flow;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.conf.Configuration;

/**
 * Generates the data/entities for the FlowRun and FlowActivity Tables.
 */
public final class TestFlowDataGenerator {
  private TestFlowDataGenerator() {
  }

  private static final String METRIC_1 = "MAP_SLOT_MILLIS";
  private static final String METRIC_2 = "HDFS_BYTES_READ";
  public static final long END_TS_INCR = 10000L;

  public static TimelineEntity getEntityMetricsApp1(long insertTs,
      Configuration c1) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunMetrics_test";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);

    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId(METRIC_1);
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    long ts = insertTs;

    for (int k = 1; k < 100; k++) {
      metricValues.put(ts - k * 200000L, 20L);
    }
    metricValues.put(ts - 80000, 40L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);

    TimelineMetric m2 = new TimelineMetric();
    m2.setId(METRIC_2);
    metricValues = new HashMap<Long, Number>();
    ts = System.currentTimeMillis();
    for (int k = 1; k < 100; k++) {
      metricValues.put(ts - k*100000L, 31L);
    }

    metricValues.put(ts - 80000, 57L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues);
    metrics.add(m2);

    entity.addMetrics(metrics);
    return entity;
  }


  public static TimelineEntity getEntityMetricsApp1Complete(long insertTs,
      Configuration c1) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunMetrics_test";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);

    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId(METRIC_1);
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    long ts = insertTs;

    metricValues.put(ts - 80000, 40L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);

    TimelineMetric m2 = new TimelineMetric();
    m2.setId(METRIC_2);
    metricValues = new HashMap<Long, Number>();
    ts = insertTs;
    metricValues.put(ts - 80000, 57L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues);
    metrics.add(m2);

    entity.addMetrics(metrics);

    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    event.setTimestamp(insertTs);
    event.addInfo("done", "insertTs=" + insertTs);
    entity.addEvent(event);
    return entity;
  }


  public static TimelineEntity getEntityMetricsApp1(long insertTs) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunMetrics_test";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);

    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId(METRIC_1);
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    long ts = insertTs;
    metricValues.put(ts - 100000, 2L);
    metricValues.put(ts - 80000, 40L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);

    TimelineMetric m2 = new TimelineMetric();
    m2.setId(METRIC_2);
    metricValues = new HashMap<Long, Number>();
    ts = insertTs;
    metricValues.put(ts - 100000, 31L);
    metricValues.put(ts - 80000, 57L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues);
    metrics.add(m2);

    entity.addMetrics(metrics);
    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    long endTs = 1439379885000L;
    event.setTimestamp(endTs);
    String expKey = "foo_event_greater";
    String expVal = "test_app_greater";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);
    return entity;
  }

  public static TimelineEntity getEntityMetricsApp2(long insertTs) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunMetrics_test";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);
    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId(METRIC_1);
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    long ts = insertTs;
    metricValues.put(ts - 100000, 5L);
    metricValues.put(ts - 80000, 101L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    entity.addMetrics(metrics);
    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    long endTs = 1439379885000L;
    event.setTimestamp(endTs);
    String expKey = "foo_event_greater";
    String expVal = "test_app_greater";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);
    return entity;
  }

  public static TimelineEntity getEntity1() {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunHello";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    long cTime = 1425026901000L;
    entity.setCreatedTime(cTime);
    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId(METRIC_1);
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    long ts = System.currentTimeMillis();
    metricValues.put(ts - 120000, 100000000L);
    metricValues.put(ts - 100000, 200000000L);
    metricValues.put(ts - 80000, 300000000L);
    metricValues.put(ts - 60000, 400000000L);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 60000000000L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    entity.addMetrics(metrics);

    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event.setTimestamp(cTime);
    String expKey = "foo_event";
    Object expVal = "test";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);

    event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    long expTs = cTime + 21600000; // start time + 6hrs
    event.setTimestamp(expTs);
    event.addInfo(expKey, expVal);
    entity.addEvent(event);

    return entity;
  }

  public static TimelineEntity getAFullEntity(long ts, long endTs) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunFullEntity";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(ts);
    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId(METRIC_1);
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    metricValues.put(ts - 120000, 100000000L);
    metricValues.put(ts - 100000, 200000000L);
    metricValues.put(ts - 80000, 300000000L);
    metricValues.put(ts - 60000, 400000000L);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 60000000000L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    TimelineMetric m2 = new TimelineMetric();
    m2.setId(METRIC_2);
    metricValues = new HashMap<Long, Number>();
    metricValues.put(ts - 900000, 31L);
    metricValues.put(ts - 30000, 57L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues);
    metrics.add(m2);
    entity.addMetrics(metrics);

    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event.setTimestamp(ts);
    String expKey = "foo_event";
    Object expVal = "test";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);

    event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    long expTs = ts + 21600000; // start time + 6hrs
    event.setTimestamp(expTs);
    event.addInfo(expKey, expVal);
    entity.addEvent(event);

    return entity;
  }

  public static TimelineEntity getEntityGreaterStartTime(long startTs) {
    TimelineEntity entity = new TimelineEntity();
    entity.setCreatedTime(startTs);
    entity.setId("flowRunHello with greater start time");
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setType(type);
    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event.setTimestamp(startTs);
    String expKey = "foo_event_greater";
    String expVal = "test_app_greater";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);
    return entity;
  }

  public static TimelineEntity getEntityMaxEndTime(long endTs) {
    TimelineEntity entity = new TimelineEntity();
    entity.setId("flowRunHello Max End time");
    entity.setType(TimelineEntityType.YARN_APPLICATION.toString());
    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    event.setTimestamp(endTs);
    String expKey = "foo_even_max_ finished";
    String expVal = "test_app_max_finished";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);
    return entity;
  }

  public static TimelineEntity getEntityMinStartTime(long startTs) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunHelloMInStartTime";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(startTs);
    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event.setTimestamp(startTs);
    entity.addEvent(event);
    return entity;
  }

  public static TimelineEntity getMinFlushEntity(long startTs) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunHelloFlushEntityMin";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(startTs);
    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event.setTimestamp(startTs);
    entity.addEvent(event);
    return entity;
  }

  public static TimelineEntity getMaxFlushEntity(long startTs) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowRunHelloFlushEntityMax";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(startTs);

    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    event.setTimestamp(startTs + END_TS_INCR);
    entity.addEvent(event);
    return entity;
  }

  public static TimelineEntity getFlowApp1(long appCreatedTime) {
    TimelineEntity entity = new TimelineEntity();
    String id = "flowActivity_test";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(appCreatedTime);

    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event.setTimestamp(appCreatedTime);
    String expKey = "foo_event";
    Object expVal = "test";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);

    return entity;
  }
}
