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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;

/**
 * Utility class that creates the schema and generates test data.
 */
public final class DataGeneratorForTest {

  // private constructor for utility class
  private DataGeneratorForTest() {
  }

   /**
   * Creates the schema for timeline service.
   * @param conf
   * @throws IOException
   */
  public static void createSchema(final Configuration conf)
      throws IOException {
    // set the jar location to null so that
    // the coprocessor class is loaded from classpath
    conf.set(YarnConfiguration.FLOW_RUN_COPROCESSOR_JAR_HDFS_LOCATION, " ");
    // now create all tables
    TimelineSchemaCreator.createAllTables(conf, false);
  }

  public static void loadApps(HBaseTestingUtility util, long ts)
      throws IOException {
    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "application_1111111111_2222";
    entity.setId(id);
    entity.setType(TimelineEntityType.YARN_APPLICATION.toString());
    Long cTime = 1425016502000L;
    entity.setCreatedTime(cTime);
    // add the info map in Timeline Entity
    entity.addInfo(getInfoMap3());
    // add the isRelatedToEntity info
    Set<String> isRelatedToSet = new HashSet<>();
    isRelatedToSet.add("relatedto1");
    Map<String, Set<String>> isRelatedTo = new HashMap<>();
    isRelatedTo.put("task", isRelatedToSet);
    entity.setIsRelatedToEntities(isRelatedTo);
    // add the relatesTo info
    Set<String> relatesToSet = new HashSet<>();
    relatesToSet.add("relatesto1");
    relatesToSet.add("relatesto3");
    Map<String, Set<String>> relatesTo = new HashMap<>();
    relatesTo.put("container", relatesToSet);
    Set<String> relatesToSet11 = new HashSet<>();
    relatesToSet11.add("relatesto4");
    relatesTo.put("container1", relatesToSet11);
    entity.setRelatesToEntities(relatesTo);
    // add some config entries
    Map<String, String> conf = new HashMap<>();
    conf.put("config_param1", "value1");
    conf.put("config_param2", "value2");
    conf.put("cfg_param1", "value3");
    entity.addConfigs(conf);
    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    metrics.add(getMetric4(ts));

    TimelineMetric m12 = new TimelineMetric();
    m12.setId("MAP1_BYTES");
    m12.addValue(ts, 50);
    metrics.add(m12);
    entity.addMetrics(metrics);
    entity.addEvent(addStartEvent(ts));
    te.addEntity(entity);
    TimelineEntities te1 = new TimelineEntities();
    TimelineEntity entity1 = new TimelineEntity();
    String id1 = "application_1111111111_3333";
    entity1.setId(id1);
    entity1.setType(TimelineEntityType.YARN_APPLICATION.toString());
    entity1.setCreatedTime(cTime + 20L);
    // add the info map in Timeline Entity
    entity1.addInfo(getInfoMap4());

    // add the isRelatedToEntity info
    Set<String> isRelatedToSet1 = new HashSet<>();
    isRelatedToSet1.add("relatedto3");
    isRelatedToSet1.add("relatedto5");
    Map<String, Set<String>> isRelatedTo1 = new HashMap<>();
    isRelatedTo1.put("task1", isRelatedToSet1);
    Set<String> isRelatedToSet11 = new HashSet<>();
    isRelatedToSet11.add("relatedto4");
    isRelatedTo1.put("task2", isRelatedToSet11);
    entity1.setIsRelatedToEntities(isRelatedTo1);

    // add the relatesTo info
    Set<String> relatesToSet1 = new HashSet<>();
    relatesToSet1.add("relatesto1");
    relatesToSet1.add("relatesto2");
    Map<String, Set<String>> relatesTo1 = new HashMap<>();
    relatesTo1.put("container", relatesToSet1);
    entity1.setRelatesToEntities(relatesTo1);

    // add some config entries
    Map<String, String> conf1 = new HashMap<>();
    conf1.put("cfg_param1", "value1");
    conf1.put("cfg_param2", "value2");
    entity1.addConfigs(conf1);

    // add metrics
    entity1.addMetrics(getMetrics4(ts));
    TimelineEvent event11 = new TimelineEvent();
    event11.setId("end_event");
    event11.setTimestamp(ts);
    entity1.addEvent(event11);
    TimelineEvent event12 = new TimelineEvent();
    event12.setId("update_event");
    event12.setTimestamp(ts - 10);
    entity1.addEvent(event12);
    te1.addEntity(entity1);

    TimelineEntities te2 = new TimelineEntities();
    te2.addEntity(getEntity4(cTime, ts));
    HBaseTimelineWriterImpl hbi = null;
    try {
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(util.getConfiguration());
      hbi.start();
      UserGroupInformation remoteUser =
          UserGroupInformation.createRemoteUser("user1");
      hbi.write(
          new TimelineCollectorContext("cluster1", "user1", "some_flow_name",
              "AB7822C10F1111", 1002345678919L, "application_1111111111_2222"),
          te, remoteUser);
      hbi.write(
          new TimelineCollectorContext("cluster1", "user1", "some_flow_name",
              "AB7822C10F1111", 1002345678919L, "application_1111111111_3333"),
          te1, remoteUser);
      hbi.write(
          new TimelineCollectorContext("cluster1", "user1", "some_flow_name",
              "AB7822C10F1111", 1002345678919L, "application_1111111111_4444"),
          te2, remoteUser);
      hbi.stop();
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  private static Set<TimelineMetric> getMetrics4(long ts) {
    Set<TimelineMetric> metrics1 = new HashSet<>();
    TimelineMetric m2 = new TimelineMetric();
    m2.setId("MAP1_SLOT_MILLIS");
    Map<Long, Number> metricValues1 = new HashMap<>();
    metricValues1.put(ts - 120000, 100000000);
    metricValues1.put(ts - 100000, 200000000);
    metricValues1.put(ts - 80000, 300000000);
    metricValues1.put(ts - 60000, 400000000);
    metricValues1.put(ts - 40000, 50000000000L);
    metricValues1.put(ts - 20000, 60000000000L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues1);
    metrics1.add(m2);
    return metrics1;
  }

  private static TimelineEntity getEntity4(long cTime, long ts) {
    TimelineEntity entity2 = new TimelineEntity();
    String id2 = "application_1111111111_4444";
    entity2.setId(id2);
    entity2.setType(TimelineEntityType.YARN_APPLICATION.toString());
    entity2.setCreatedTime(cTime + 40L);
    TimelineEvent event21 = new TimelineEvent();
    event21.setId("update_event");
    event21.setTimestamp(ts - 20);
    entity2.addEvent(event21);
    Set<String> isRelatedToSet2 = new HashSet<String>();
    isRelatedToSet2.add("relatedto3");
    Map<String, Set<String>> isRelatedTo2 = new HashMap<>();
    isRelatedTo2.put("task1", isRelatedToSet2);
    entity2.setIsRelatedToEntities(isRelatedTo2);
    Map<String, Set<String>> relatesTo3 = new HashMap<>();
    Set<String> relatesToSet14 = new HashSet<String>();
    relatesToSet14.add("relatesto7");
    relatesTo3.put("container2", relatesToSet14);
    entity2.setRelatesToEntities(relatesTo3);
    return entity2;
  }

  private static Map<String, Object> getInfoMap4() {
    Map<String, Object> infoMap1 = new HashMap<>();
    infoMap1.put("infoMapKey1", "infoMapValue1");
    infoMap1.put("infoMapKey2", 10);
    return infoMap1;
  }

  private static TimelineMetric getMetric4(long ts) {
    TimelineMetric m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    Map<Long, Number> metricValues = new HashMap<>();
    metricValues.put(ts - 120000, 100000000);
    metricValues.put(ts - 100000, 200000000);
    metricValues.put(ts - 80000, 300000000);
    metricValues.put(ts - 60000, 400000000);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 60000000000L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    return m1;
  }

  private static Map<String, Object> getInfoMap3() {
    Map<String, Object> infoMap = new HashMap<>();
    infoMap.put("infoMapKey1", "infoMapValue2");
    infoMap.put("infoMapKey2", 20);
    infoMap.put("infoMapKey3", 85.85);
    return infoMap;
  }

  private static Map<String, Object> getInfoMap1() {
    Map<String, Object> infoMap = new HashMap<>();
    infoMap.put("infoMapKey1", "infoMapValue2");
    infoMap.put("infoMapKey2", 20);
    infoMap.put("infoMapKey3", 71.4);
    return infoMap;
  }

  private static Map<String, Set<String>> getRelatesTo1() {
    Set<String> relatesToSet = new HashSet<String>();
    relatesToSet.add("relatesto1");
    relatesToSet.add("relatesto3");
    Map<String, Set<String>> relatesTo = new HashMap<>();
    relatesTo.put("container", relatesToSet);
    Set<String> relatesToSet11 = new HashSet<>();
    relatesToSet11.add("relatesto4");
    relatesTo.put("container1", relatesToSet11);
    return relatesTo;
  }

  private static Map<String, String> getConfig1() {
    Map<String, String> conf = new HashMap<>();
    conf.put("config_param1", "value1");
    conf.put("config_param2", "value2");
    conf.put("cfg_param1", "value3");
    return conf;
  }

  private static Map<String, String> getConfig2() {
    Map<String, String> conf1 = new HashMap<>();
    conf1.put("cfg_param1", "value1");
    conf1.put("cfg_param2", "value2");
    return conf1;
  }

  private static Map<String, Object> getInfoMap2() {
    Map<String, Object> infoMap1 = new HashMap<>();
    infoMap1.put("infoMapKey1", "infoMapValue1");
    infoMap1.put("infoMapKey2", 10);
    return infoMap1;
  }

  private static Map<String, Set<String>> getIsRelatedTo1() {
    Set<String> isRelatedToSet = new HashSet<>();
    isRelatedToSet.add("relatedto1");
    Map<String, Set<String>> isRelatedTo = new HashMap<>();
    isRelatedTo.put("task", isRelatedToSet);
    return isRelatedTo;
  }

  private static Map<Long, Number> getMetricValues1(long ts) {
    Map<Long, Number> metricValues = new HashMap<>();
    metricValues.put(ts - 120000, 100000000);
    metricValues.put(ts - 100000, 200000000);
    metricValues.put(ts - 80000, 300000000);
    metricValues.put(ts - 60000, 400000000);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 70000000000L);
    return metricValues;
  }

  public static void loadEntities(HBaseTestingUtility util, long ts)
      throws IOException {
    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "hello";
    String type = "world";
    entity.setId(id);
    entity.setType(type);
    Long cTime = 1425016502000L;
    entity.setCreatedTime(cTime);
    // add the info map in Timeline Entity
    entity.addInfo(getInfoMap1());
    // add the isRelatedToEntity info
    entity.setIsRelatedToEntities(getIsRelatedTo1());

    // add the relatesTo info
    entity.setRelatesToEntities(getRelatesTo1());

    // add some config entries
    entity.addConfigs(getConfig1());

    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    m1.setType(Type.TIME_SERIES);
    m1.setValues(getMetricValues1(ts));
    metrics.add(m1);

    TimelineMetric m12 = new TimelineMetric();
    m12.setId("MAP1_BYTES");
    m12.addValue(ts, 50);
    metrics.add(m12);
    entity.addMetrics(metrics);
    entity.addEvent(addStartEvent(ts));
    te.addEntity(entity);

    TimelineEntity entity1 = new TimelineEntity();
    String id1 = "hello1";
    entity1.setId(id1);
    entity1.setType(type);
    entity1.setCreatedTime(cTime + 20L);

    // add the info map in Timeline Entity
    entity1.addInfo(getInfoMap2());

    // add event.
    TimelineEvent event11 = new TimelineEvent();
    event11.setId("end_event");
    event11.setTimestamp(ts);
    entity1.addEvent(event11);
    TimelineEvent event12 = new TimelineEvent();
    event12.setId("update_event");
    event12.setTimestamp(ts - 10);
    entity1.addEvent(event12);


    // add the isRelatedToEntity info
    entity1.setIsRelatedToEntities(getIsRelatedTo2());

    // add the relatesTo info
    Set<String> relatesToSet1 = new HashSet<String>();
    relatesToSet1.add("relatesto1");
    relatesToSet1.add("relatesto2");
    Map<String, Set<String>> relatesTo1 = new HashMap<>();
    relatesTo1.put("container", relatesToSet1);
    entity1.setRelatesToEntities(relatesTo1);

    // add some config entries
    entity1.addConfigs(getConfig2());

    // add metrics
    Set<TimelineMetric> metrics1 = new HashSet<>();
    TimelineMetric m2 = new TimelineMetric();
    m2.setId("MAP1_SLOT_MILLIS");
    m2.setType(Type.TIME_SERIES);
    m2.setValues(getMetricValues2(ts));
    metrics1.add(m2);
    entity1.addMetrics(metrics1);
    te.addEntity(entity1);

    te.addEntity(getEntity2(type, cTime, ts));

    // For listing types
    for (int i = 0; i < 10; i++) {
      TimelineEntity entity3 = new TimelineEntity();
      String id3 = "typeTest" + i;
      entity3.setId(id3);
      StringBuilder typeName = new StringBuilder("newType");
      for (int j = 0; j < (i % 3); j++) {
        typeName.append(" ").append(j);
      }
      entity3.setType(typeName.toString());
      entity3.setCreatedTime(cTime + 80L + i);
      te.addEntity(entity3);
    }

    // Create app entity for app to flow table
    TimelineEntities appTe1 = new TimelineEntities();
    TimelineEntity entityApp1 = new TimelineEntity();
    String appName1 = "application_1231111111_1111";
    entityApp1.setId(appName1);
    entityApp1.setType(TimelineEntityType.YARN_APPLICATION.toString());
    entityApp1.setCreatedTime(cTime + 40L);
    TimelineEvent appCreationEvent1 = new TimelineEvent();
    appCreationEvent1.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    appCreationEvent1.setTimestamp(cTime);
    entityApp1.addEvent(appCreationEvent1);
    appTe1.addEntity(entityApp1);

    TimelineEntities appTe2 = new TimelineEntities();
    TimelineEntity entityApp2 = new TimelineEntity();
    String appName2 = "application_1231111111_1112";
    entityApp2.setId(appName2);
    entityApp2.setType(TimelineEntityType.YARN_APPLICATION.toString());
    entityApp2.setCreatedTime(cTime + 50L);
    TimelineEvent appCreationEvent2 = new TimelineEvent();
    appCreationEvent2.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    appCreationEvent2.setTimestamp(cTime);
    entityApp2.addEvent(appCreationEvent2);
    appTe2.addEntity(entityApp2);

    HBaseTimelineWriterImpl hbi = null;
    try {
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(util.getConfiguration());
      hbi.start();

      UserGroupInformation user =
          UserGroupInformation.createRemoteUser("user1");
      TimelineCollectorContext context =
          new TimelineCollectorContext("cluster1", "user1", "some_flow_name",
              "AB7822C10F1111", 1002345678919L, appName1);
      hbi.write(context, te, user);
      hbi.write(context, appTe1, user);

      context = new TimelineCollectorContext("cluster1", "user1",
          "some_flow_name", "AB7822C10F1111", 1002345678919L, appName2);
      hbi.write(context, te, user);
      hbi.write(context, appTe2, user);
      hbi.stop();
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  private static TimelineEntity getEntity2(String type, long cTime,
      long ts) {
    TimelineEntity entity2 = new TimelineEntity();
    String id2 = "hello2";
    entity2.setId(id2);
    entity2.setType(type);
    entity2.setCreatedTime(cTime + 40L);
    TimelineEvent event21 = new TimelineEvent();
    event21.setId("update_event");
    event21.setTimestamp(ts - 20);
    entity2.addEvent(event21);
    Set<String> isRelatedToSet2 = new HashSet<>();
    isRelatedToSet2.add("relatedto3");
    Map<String, Set<String>> isRelatedTo2 = new HashMap<>();
    isRelatedTo2.put("task1", isRelatedToSet2);
    entity2.setIsRelatedToEntities(isRelatedTo2);
    Map<String, Set<String>> relatesTo3 = new HashMap<>();
    Set<String> relatesToSet14 = new HashSet<>();
    relatesToSet14.add("relatesto7");
    relatesTo3.put("container2", relatesToSet14);
    entity2.setRelatesToEntities(relatesTo3);
    return entity2;
  }

  private static TimelineEvent addStartEvent(long ts) {
    TimelineEvent event = new TimelineEvent();
    event.setId("start_event");
    event.setTimestamp(ts);
    return event;
  }

  private static Map<Long, Number> getMetricValues2(long ts1) {
    Map<Long, Number> metricValues1 = new HashMap<>();
    metricValues1.put(ts1 - 120000, 100000000);
    metricValues1.put(ts1 - 100000, 200000000);
    metricValues1.put(ts1 - 80000, 300000000);
    metricValues1.put(ts1 - 60000, 400000000);
    metricValues1.put(ts1 - 40000, 50000000000L);
    metricValues1.put(ts1 - 20000, 60000000000L);
    return metricValues1;
  }

  private static Map<String, Set<String>> getIsRelatedTo2() {
    Set<String> isRelatedToSet1 = new HashSet<>();
    isRelatedToSet1.add("relatedto3");
    isRelatedToSet1.add("relatedto5");
    Map<String, Set<String>> isRelatedTo1 = new HashMap<>();
    isRelatedTo1.put("task1", isRelatedToSet1);
    Set<String> isRelatedToSet11 = new HashSet<>();
    isRelatedToSet11.add("relatedto4");
    isRelatedTo1.put("task2", isRelatedToSet11);
    return isRelatedTo1;
  }
}
