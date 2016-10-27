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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;

final class DataGeneratorForTest {
  static void loadApps(HBaseTestingUtility util) throws IOException {
    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "application_1111111111_2222";
    entity.setId(id);
    entity.setType(TimelineEntityType.YARN_APPLICATION.toString());
    Long cTime = 1425016502000L;
    entity.setCreatedTime(cTime);
    // add the info map in Timeline Entity
    Map<String, Object> infoMap = new HashMap<>();
    infoMap.put("infoMapKey1", "infoMapValue2");
    infoMap.put("infoMapKey2", 20);
    infoMap.put("infoMapKey3", 85.85);
    entity.addInfo(infoMap);
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
    TimelineMetric m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    Map<Long, Number> metricValues = new HashMap<>();
    long ts = System.currentTimeMillis();
    metricValues.put(ts - 120000, 100000000);
    metricValues.put(ts - 100000, 200000000);
    metricValues.put(ts - 80000, 300000000);
    metricValues.put(ts - 60000, 400000000);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 60000000000L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);

    TimelineMetric m12 = new TimelineMetric();
    m12.setId("MAP1_BYTES");
    m12.addValue(ts, 50);
    metrics.add(m12);
    entity.addMetrics(metrics);
    TimelineEvent event = new TimelineEvent();
    event.setId("start_event");
    event.setTimestamp(ts);
    entity.addEvent(event);
    te.addEntity(entity);
    TimelineEntities te1 = new TimelineEntities();
    TimelineEntity entity1 = new TimelineEntity();
    String id1 = "application_1111111111_3333";
    entity1.setId(id1);
    entity1.setType(TimelineEntityType.YARN_APPLICATION.toString());
    entity1.setCreatedTime(cTime + 20L);
    // add the info map in Timeline Entity
    Map<String, Object> infoMap1 = new HashMap<>();
    infoMap1.put("infoMapKey1", "infoMapValue1");
    infoMap1.put("infoMapKey2", 10);
    entity1.addInfo(infoMap1);

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
    Set<TimelineMetric> metrics1 = new HashSet<>();
    TimelineMetric m2 = new TimelineMetric();
    m2.setId("MAP1_SLOT_MILLIS");
    Map<Long, Number> metricValues1 = new HashMap<>();
    long ts1 = System.currentTimeMillis();
    metricValues1.put(ts1 - 120000, 100000000);
    metricValues1.put(ts1 - 100000, 200000000);
    metricValues1.put(ts1 - 80000, 300000000);
    metricValues1.put(ts1 - 60000, 400000000);
    metricValues1.put(ts1 - 40000, 50000000000L);
    metricValues1.put(ts1 - 20000, 60000000000L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues1);
    metrics1.add(m2);
    entity1.addMetrics(metrics1);
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

    te2.addEntity(entity2);
    HBaseTimelineWriterImpl hbi = null;
    try {
      hbi = new HBaseTimelineWriterImpl(util.getConfiguration());
      hbi.init(util.getConfiguration());
      hbi.start();
      String cluster = "cluster1";
      String user = "user1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      String appName = "application_1111111111_2222";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      appName = "application_1111111111_3333";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te1);
      appName = "application_1111111111_4444";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te2);
      hbi.stop();
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  static void loadEntities(HBaseTestingUtility util) throws IOException {
    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "hello";
    String type = "world";
    entity.setId(id);
    entity.setType(type);
    Long cTime = 1425016502000L;
    entity.setCreatedTime(cTime);
    // add the info map in Timeline Entity
    Map<String, Object> infoMap = new HashMap<>();
    infoMap.put("infoMapKey1", "infoMapValue2");
    infoMap.put("infoMapKey2", 20);
    infoMap.put("infoMapKey3", 71.4);
    entity.addInfo(infoMap);
    // add the isRelatedToEntity info
    Set<String> isRelatedToSet = new HashSet<>();
    isRelatedToSet.add("relatedto1");
    Map<String, Set<String>> isRelatedTo = new HashMap<>();
    isRelatedTo.put("task", isRelatedToSet);
    entity.setIsRelatedToEntities(isRelatedTo);

    // add the relatesTo info
    Set<String> relatesToSet = new HashSet<String>();
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
    TimelineMetric m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    Map<Long, Number> metricValues = new HashMap<>();
    long ts = System.currentTimeMillis();
    metricValues.put(ts - 120000, 100000000);
    metricValues.put(ts - 100000, 200000000);
    metricValues.put(ts - 80000, 300000000);
    metricValues.put(ts - 60000, 400000000);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 70000000000L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);

    TimelineMetric m12 = new TimelineMetric();
    m12.setId("MAP1_BYTES");
    m12.addValue(ts, 50);
    metrics.add(m12);
    entity.addMetrics(metrics);
    TimelineEvent event = new TimelineEvent();
    event.setId("start_event");
    event.setTimestamp(ts);
    entity.addEvent(event);
    te.addEntity(entity);

    TimelineEntity entity1 = new TimelineEntity();
    String id1 = "hello1";
    entity1.setId(id1);
    entity1.setType(type);
    entity1.setCreatedTime(cTime + 20L);

    // add the info map in Timeline Entity
    Map<String, Object> infoMap1 = new HashMap<>();
    infoMap1.put("infoMapKey1", "infoMapValue1");
    infoMap1.put("infoMapKey2", 10);
    entity1.addInfo(infoMap1);

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
    Set<String> relatesToSet1 = new HashSet<String>();
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
    Set<TimelineMetric> metrics1 = new HashSet<>();
    TimelineMetric m2 = new TimelineMetric();
    m2.setId("MAP1_SLOT_MILLIS");
    Map<Long, Number> metricValues1 = new HashMap<>();
    long ts1 = System.currentTimeMillis();
    metricValues1.put(ts1 - 120000, 100000000);
    metricValues1.put(ts1 - 100000, 200000000);
    metricValues1.put(ts1 - 80000, 300000000);
    metricValues1.put(ts1 - 60000, 400000000);
    metricValues1.put(ts1 - 40000, 50000000000L);
    metricValues1.put(ts1 - 20000, 60000000000L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues1);
    metrics1.add(m2);
    entity1.addMetrics(metrics1);
    te.addEntity(entity1);

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
    te.addEntity(entity2);
    HBaseTimelineWriterImpl hbi = null;
    try {
      hbi = new HBaseTimelineWriterImpl(util.getConfiguration());
      hbi.init(util.getConfiguration());
      hbi.start();
      String cluster = "cluster1";
      String user = "user1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      String appName = "application_1231111111_1111";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      hbi.stop();
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }
}
