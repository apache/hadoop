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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationAttemptEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileSystemTimelineReaderImpl {

  private static final String ROOT_DIR = new File("target",
      TestFileSystemTimelineReaderImpl.class.getSimpleName()).getAbsolutePath();
  private FileSystemTimelineReaderImpl reader;

  @BeforeClass
  public static void setup() throws Exception {
    initializeDataDirectory(ROOT_DIR);
  }

  public static void initializeDataDirectory(String rootDir) throws Exception {
    loadEntityData(rootDir);
    // Create app flow mapping file.
    CSVFormat format =
        CSVFormat.DEFAULT.withHeader("APP", "USER", "FLOW", "FLOWRUN");
    String appFlowMappingFile = rootDir + File.separator + "entities" +
        File.separator + "cluster1" + File.separator +
        FileSystemTimelineReaderImpl.APP_FLOW_MAPPING_FILE;
    try (PrintWriter out =
        new PrintWriter(new BufferedWriter(
            new FileWriter(appFlowMappingFile, true)));
        CSVPrinter printer = new CSVPrinter(out, format)){
      printer.printRecord("app1", "user1", "flow1", 1);
      printer.printRecord("app2", "user1", "flow1,flow", 1);
      printer.close();
    }
    (new File(rootDir)).deleteOnExit();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(ROOT_DIR));
  }

  @Before
  public void init() throws Exception {
    reader = new FileSystemTimelineReaderImpl();
    Configuration conf = new YarnConfiguration();
    conf.set(FileSystemTimelineReaderImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        ROOT_DIR);
    reader.init(conf);
  }

  private static void writeEntityFile(TimelineEntity entity, File dir)
      throws Exception {
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IOException("Could not create directories for " + dir);
      }
    }
    String fileName = dir.getAbsolutePath() + File.separator + entity.getId() +
        ".thist";
    try (PrintWriter out =
        new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))){
      out.println(TimelineUtils.dumpTimelineRecordtoJSON(entity));
      out.write("\n");
      out.close();
    }
  }

  private static void loadEntityData(String rootDir) throws Exception {
    File appDir =
        getAppDir(rootDir, "cluster1", "user1", "flow1", "1", "app1", "app");
    TimelineEntity entity11 = new TimelineEntity();
    entity11.setId("id_1");
    entity11.setType("app");
    entity11.setCreatedTime(1425016502000L);
    Map<String, Object> info1 = new HashMap<String, Object>();
    info1.put("info1", "val1");
    info1.put("info2", "val5");
    entity11.addInfo(info1);
    TimelineEvent event = new TimelineEvent();
    event.setId("event_1");
    event.setTimestamp(1425016502003L);
    entity11.addEvent(event);
    Set<TimelineMetric> metrics = new HashSet<TimelineMetric>();
    TimelineMetric metric1 = new TimelineMetric();
    metric1.setId("metric1");
    metric1.setType(TimelineMetric.Type.SINGLE_VALUE);
    metric1.addValue(1425016502006L, 113);
    metrics.add(metric1);
    TimelineMetric metric2 = new TimelineMetric();
    metric2.setId("metric2");
    metric2.setType(TimelineMetric.Type.TIME_SERIES);
    metric2.addValue(1425016502016L, 34);
    metrics.add(metric2);
    entity11.setMetrics(metrics);
    Map<String, String> configs = new HashMap<String, String>();
    configs.put("config_1", "127");
    entity11.setConfigs(configs);
    entity11.addRelatesToEntity("flow", "flow1");
    entity11.addIsRelatedToEntity("type1", "tid1_1");
    writeEntityFile(entity11, appDir);
    TimelineEntity entity12 = new TimelineEntity();
    entity12.setId("id_1");
    entity12.setType("app");
    configs.clear();
    configs.put("config_2", "23");
    configs.put("config_3", "abc");
    entity12.addConfigs(configs);
    metrics.clear();
    TimelineMetric metric12 = new TimelineMetric();
    metric12.setId("metric2");
    metric12.setType(TimelineMetric.Type.TIME_SERIES);
    metric12.addValue(1425016502032L, 48);
    metric12.addValue(1425016502054L, 51);
    metrics.add(metric12);
    TimelineMetric metric3 = new TimelineMetric();
    metric3.setId("metric3");
    metric3.setType(TimelineMetric.Type.SINGLE_VALUE);
    metric3.addValue(1425016502060L, 23L);
    metrics.add(metric3);
    entity12.setMetrics(metrics);
    entity12.addIsRelatedToEntity("type1", "tid1_2");
    entity12.addIsRelatedToEntity("type2", "tid2_1`");
    TimelineEvent event15 = new TimelineEvent();
    event15.setId("event_5");
    event15.setTimestamp(1425016502017L);
    entity12.addEvent(event15);
    writeEntityFile(entity12, appDir);

    TimelineEntity entity2 = new TimelineEntity();
    entity2.setId("id_2");
    entity2.setType("app");
    entity2.setCreatedTime(1425016501050L);
    Map<String, Object> info2 = new HashMap<String, Object>();
    info1.put("info2", 4);
    entity2.addInfo(info2);
    Map<String, String> configs2 = new HashMap<String, String>();
    configs2.put("config_1", "129");
    configs2.put("config_3", "def");
    entity2.setConfigs(configs2);
    TimelineEvent event2 = new TimelineEvent();
    event2.setId("event_2");
    event2.setTimestamp(1425016501003L);
    entity2.addEvent(event2);
    Set<TimelineMetric> metrics2 = new HashSet<TimelineMetric>();
    TimelineMetric metric21 = new TimelineMetric();
    metric21.setId("metric1");
    metric21.setType(TimelineMetric.Type.SINGLE_VALUE);
    metric21.addValue(1425016501006L, 300);
    metrics2.add(metric21);
    TimelineMetric metric22 = new TimelineMetric();
    metric22.setId("metric2");
    metric22.setType(TimelineMetric.Type.TIME_SERIES);
    metric22.addValue(1425016501056L, 31);
    metric22.addValue(1425016501084L, 70);
    metrics2.add(metric22);
    TimelineMetric metric23 = new TimelineMetric();
    metric23.setId("metric3");
    metric23.setType(TimelineMetric.Type.SINGLE_VALUE);
    metric23.addValue(1425016502060L, 23L);
    metrics2.add(metric23);
    entity2.setMetrics(metrics2);
    entity2.addRelatesToEntity("flow", "flow2");
    writeEntityFile(entity2, appDir);

    TimelineEntity entity3 = new TimelineEntity();
    entity3.setId("id_3");
    entity3.setType("app");
    entity3.setCreatedTime(1425016501050L);
    Map<String, Object> info3 = new HashMap<String, Object>();
    info3.put("info2", 3.5);
    info3.put("info4", 20);
    entity3.addInfo(info3);
    Map<String, String> configs3 = new HashMap<String, String>();
    configs3.put("config_1", "123");
    configs3.put("config_3", "abc");
    entity3.setConfigs(configs3);
    TimelineEvent event3 = new TimelineEvent();
    event3.setId("event_2");
    event3.setTimestamp(1425016501003L);
    entity3.addEvent(event3);
    TimelineEvent event4 = new TimelineEvent();
    event4.setId("event_4");
    event4.setTimestamp(1425016502006L);
    entity3.addEvent(event4);
    Set<TimelineMetric> metrics3 = new HashSet<TimelineMetric>();
    TimelineMetric metric31 = new TimelineMetric();
    metric31.setId("metric1");
    metric31.setType(TimelineMetric.Type.SINGLE_VALUE);
    metric31.addValue(1425016501006L, 124);
    metrics3.add(metric31);
    TimelineMetric metric32 = new TimelineMetric();
    metric32.setId("metric2");
    metric32.setType(TimelineMetric.Type.TIME_SERIES);
    metric32.addValue(1425016501056L, 31);
    metric32.addValue(1425016501084L, 74);
    metrics3.add(metric32);
    entity3.setMetrics(metrics3);
    entity3.addIsRelatedToEntity("type1", "tid1_2");
    writeEntityFile(entity3, appDir);

    TimelineEntity entity4 = new TimelineEntity();
    entity4.setId("id_4");
    entity4.setType("app");
    entity4.setCreatedTime(1425016502050L);
    TimelineEvent event44 = new TimelineEvent();
    event44.setId("event_4");
    event44.setTimestamp(1425016502003L);
    entity4.addEvent(event44);
    writeEntityFile(entity4, appDir);

    File attemptDir = getAppDir(rootDir, "cluster1", "user1", "flow1", "1",
        "app1", TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString());
    ApplicationAttemptEntity attempt1 = new ApplicationAttemptEntity();
    attempt1.setId("app-attempt-1");
    attempt1.setCreatedTime(1425017502003L);
    writeEntityFile(attempt1, attemptDir);
    ApplicationAttemptEntity attempt2 = new ApplicationAttemptEntity();
    attempt2.setId("app-attempt-2");
    attempt2.setCreatedTime(1425017502004L);
    writeEntityFile(attempt2, attemptDir);

    File entityDir = getAppDir(rootDir, "cluster1", "user1", "flow1", "1",
        "app1", TimelineEntityType.YARN_CONTAINER.toString());
    ContainerEntity containerEntity1 = new ContainerEntity();
    containerEntity1.setId("container_1_1");
    containerEntity1.setParent(attempt1.getIdentifier());
    containerEntity1.setCreatedTime(1425017502003L);
    writeEntityFile(containerEntity1, entityDir);

    ContainerEntity containerEntity2 = new ContainerEntity();
    containerEntity2.setId("container_2_1");
    containerEntity2.setParent(attempt2.getIdentifier());
    containerEntity2.setCreatedTime(1425018502003L);
    writeEntityFile(containerEntity2, entityDir);

    ContainerEntity containerEntity3 = new ContainerEntity();
    containerEntity3.setId("container_2_2");
    containerEntity3.setParent(attempt2.getIdentifier());
    containerEntity3.setCreatedTime(1425018502003L);
    writeEntityFile(containerEntity3, entityDir);

    File appDir2 =
        getAppDir(rootDir, "cluster1", "user1", "flow1,flow", "1", "app2",
            "app");
    TimelineEntity entity5 = new TimelineEntity();
    entity5.setId("id_5");
    entity5.setType("app");
    entity5.setCreatedTime(1425016502050L);
    writeEntityFile(entity5, appDir2);
  }

  private static File getAppDir(String rootDir, String cluster, String user,
      String flowName, String flowRunId, String appId, String entityName) {
    return new File(rootDir + File.separator + "entities" + File.separator +
        cluster + File.separator + user + File.separator + flowName +
        File.separator + flowRunId + File.separator + appId + File.separator +
        entityName + File.separator);
  }

  @Test
  public void testGetEntityDefaultView() throws Exception {
    // If no fields are specified, entity is returned with default view i.e.
    // only the id, type and created time.
    TimelineEntity result = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", "id_1"),
        new TimelineDataToRetrieve(null, null, null, null));
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals((Long)1425016502000L, result.getCreatedTime());
    Assert.assertEquals(0, result.getConfigs().size());
    Assert.assertEquals(0, result.getMetrics().size());
  }

  @Test
  public void testGetEntityByClusterAndApp() throws Exception {
    // Cluster and AppId should be enough to get an entity.
    TimelineEntity result = reader.getEntity(
        new TimelineReaderContext("cluster1", null, null, null, "app1", "app",
        "id_1"),
        new TimelineDataToRetrieve(null, null, null, null));
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals((Long)1425016502000L, result.getCreatedTime());
    Assert.assertEquals(0, result.getConfigs().size());
    Assert.assertEquals(0, result.getMetrics().size());
  }

  /** This test checks whether we can handle commas in app flow mapping csv. */
  @Test
  public void testAppFlowMappingCsv() throws Exception {
    // Test getting an entity by cluster and app where flow entry
    // in app flow mapping csv has commas.
    TimelineEntity result = reader.getEntity(
        new TimelineReaderContext("cluster1", null, null, null, "app2",
        "app", "id_5"),
        new TimelineDataToRetrieve(null, null, null, null));
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_5")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals((Long)1425016502050L, result.getCreatedTime());
  }

  @Test
  public void testGetEntityCustomFields() throws Exception {
    // Specified fields in addition to default view will be returned.
    TimelineEntity result = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", "id_1"),
        new TimelineDataToRetrieve(null, null,
        EnumSet.of(Field.INFO, Field.CONFIGS, Field.METRICS), null));
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals((Long)1425016502000L, result.getCreatedTime());
    Assert.assertEquals(3, result.getConfigs().size());
    Assert.assertEquals(3, result.getMetrics().size());
    Assert.assertEquals(2, result.getInfo().size());
    // No events will be returned
    Assert.assertEquals(0, result.getEvents().size());
  }

  @Test
  public void testGetEntityAllFields() throws Exception {
    // All fields of TimelineEntity will be returned.
    TimelineEntity result = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", "id_1"),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals((Long)1425016502000L, result.getCreatedTime());
    Assert.assertEquals(3, result.getConfigs().size());
    Assert.assertEquals(3, result.getMetrics().size());
    // All fields including events will be returned.
    Assert.assertEquals(2, result.getEvents().size());
  }

  @Test
  public void testGetAllEntities() throws Exception {
    Set<TimelineEntity> result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null), new TimelineEntityFilters(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
    // All 4 entities will be returned
    Assert.assertEquals(4, result.size());
  }

  @Test
  public void testGetEntitiesWithLimit() throws Exception {
    Set<TimelineEntity> result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(2L, null, null, null, null, null, null,
        null, null), new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    // Needs to be rewritten once hashcode and equals for
    // TimelineEntity is implemented
    // Entities with id_1 and id_4 should be returned,
    // based on created time, descending.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_4")) {
        Assert.fail("Entity not sorted by created time");
      }
    }
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(3L, null, null, null, null, null, null,
        null, null), new TimelineDataToRetrieve());
    // Even though 2 entities out of 4 have same created time, one entity
    // is left out due to limit
    Assert.assertEquals(3, result.size());
  }

  @Test
  public void testGetEntitiesByTimeWindows() throws Exception {
    // Get entities based on created time start and end time range.
    Set<TimelineEntity> result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, 1425016502030L, 1425016502060L, null,
        null, null, null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    // Only one entity with ID id_4 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on created time range");
      }
    }

    // Get entities if only created time end is specified.
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
            "app", null),
            new TimelineEntityFilters(null, null, 1425016502010L, null, null,
            null, null, null, null),
            new TimelineDataToRetrieve());
    Assert.assertEquals(3, result.size());
    for (TimelineEntity entity : result) {
      if (entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on created time range");
      }
    }

    // Get entities if only created time start is specified.
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, 1425016502010L, null, null, null,
        null, null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on created time range");
      }
    }
  }

  @Test
  public void testGetFilteredEntities() throws Exception {
    // Get entities based on info filters.
    TimelineFilterList infoFilterList = new TimelineFilterList();
    infoFilterList.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "info2", 3.5));
    Set<TimelineEntity> result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList,
        null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    // Only one entity with ID id_3 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }

    // Get entities based on config filters.
    TimelineFilterList confFilterList = new TimelineFilterList();
    confFilterList.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "config_1", "123"));
    confFilterList.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "config_3", "abc"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on config filters");
      }
    }

    // Get entities based on event filters.
    TimelineFilterList eventFilters = new TimelineFilterList();
    eventFilters.addFilter(
        new TimelineExistsFilter(TimelineCompareOp.EQUAL, "event_2"));
    eventFilters.addFilter(
        new TimelineExistsFilter(TimelineCompareOp.EQUAL, "event_4"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        null, eventFilters),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on event filters");
      }
    }

    // Get entities based on metric filters.
    TimelineFilterList metricFilterList = new TimelineFilterList();
    metricFilterList.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_OR_EQUAL, "metric3", 0L));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    // Two entities with IDs' id_1 and id_2 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on metric filters");
      }
    }

    // Get entities based on complex config filters.
    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "config_1", "129"));
    list1.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "config_3", "def"));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "config_2", "23"));
    list2.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "config_3", "abc"));
    TimelineFilterList confFilterList1 =
        new TimelineFilterList(Operator.OR, list1, list2);
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList1, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on config filters");
      }
    }

    TimelineFilterList list3 = new TimelineFilterList();
    list3.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "config_1", "123"));
    list3.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "config_3", "abc"));
    TimelineFilterList list4 = new TimelineFilterList();
    list4.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "config_2", "23"));
    TimelineFilterList confFilterList2 =
        new TimelineFilterList(Operator.OR, list3, list4);
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList2, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on config filters");
      }
    }

    TimelineFilterList confFilterList3 = new TimelineFilterList();
    confFilterList3.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "config_1", "127"));
    confFilterList3.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "config_3", "abc"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList3, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    for(TimelineEntity entity : result) {
      if (!entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on config filters");
      }
    }

    TimelineFilterList confFilterList4 = new TimelineFilterList();
    confFilterList4.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "config_dummy", "dummy"));
    confFilterList4.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "config_3", "def"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList4, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(0, result.size());

    TimelineFilterList confFilterList5 = new TimelineFilterList(Operator.OR);
    confFilterList5.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "config_dummy", "dummy"));
    confFilterList5.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "config_3", "def"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList5, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on config filters");
      }
    }

    // Get entities based on complex metric filters.
    TimelineFilterList list6 = new TimelineFilterList();
    list6.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_THAN, "metric1", 200));
    list6.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.EQUAL, "metric3", 23));
    TimelineFilterList list7 = new TimelineFilterList();
    list7.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_OR_EQUAL, "metric2", 74));
    TimelineFilterList metricFilterList1 =
        new TimelineFilterList(Operator.OR, list6, list7);
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList1, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    // Two entities with IDs' id_2 and id_3 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_2") && !entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on metric filters");
      }
    }

    TimelineFilterList metricFilterList2 = new TimelineFilterList();
    metricFilterList2.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_THAN, "metric2", 70));
    metricFilterList2.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_OR_EQUAL, "metric3", 23));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList2, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1")) {
        Assert.fail("Incorrect filtering based on metric filters");
      }
    }

    TimelineFilterList metricFilterList3 = new TimelineFilterList();
    metricFilterList3.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_THAN, "dummy_metric", 30));
    metricFilterList3.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_OR_EQUAL, "metric3", 23));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList3, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(0, result.size());

    TimelineFilterList metricFilterList4 = new TimelineFilterList(Operator.OR);
    metricFilterList4.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_THAN, "dummy_metric", 30));
    metricFilterList4.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_OR_EQUAL, "metric3", 23));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList4, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on metric filters");
      }
    }

    TimelineFilterList metricFilterList5 =
        new TimelineFilterList(new TimelineCompareFilter(
            TimelineCompareOp.NOT_EQUAL, "metric2", 74));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList5, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on metric filters");
      }
    }

    TimelineFilterList infoFilterList1 = new TimelineFilterList();
    infoFilterList1.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "info2", 3.5));
    infoFilterList1.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.NOT_EQUAL, "info4", 20));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList1,
        null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(0, result.size());

    TimelineFilterList infoFilterList2 = new TimelineFilterList(Operator.OR);
    infoFilterList2.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "info2", 3.5));
    infoFilterList2.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "info1", "val1"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList2,
        null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }

    TimelineFilterList infoFilterList3 = new TimelineFilterList();
    infoFilterList3.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "dummy_info", 1));
    infoFilterList3.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "info2", "val5"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList3,
        null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(0, result.size());

    TimelineFilterList infoFilterList4 = new TimelineFilterList(Operator.OR);
    infoFilterList4.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "dummy_info", 1));
    infoFilterList4.addFilter(
        new TimelineKeyValueFilter(TimelineCompareOp.EQUAL, "info2", "val5"));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList4,
        null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }
  }

  @Test
  public void testGetEntitiesByRelations() throws Exception {
    // Get entities based on relatesTo.
    TimelineFilterList relatesTo = new TimelineFilterList(Operator.OR);
    Set<Object> relatesToIds =
        new HashSet<Object>(Arrays.asList((Object)"flow1"));
    relatesTo.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "flow", relatesToIds));
    Set<TimelineEntity> result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, relatesTo, null, null,
        null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(1, result.size());
    // Only one entity with ID id_1 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1")) {
        Assert.fail("Incorrect filtering based on relatesTo");
      }
    }

    // Get entities based on isRelatedTo.
    TimelineFilterList isRelatedTo = new TimelineFilterList(Operator.OR);
    Set<Object> isRelatedToIds =
        new HashSet<Object>(Arrays.asList((Object)"tid1_2"));
    isRelatedTo.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "type1", isRelatedToIds));
    result = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "flow1", 1L, "app1",
        "app", null),
        new TimelineEntityFilters(null, null, null, null, isRelatedTo, null,
        null, null, null),
        new TimelineDataToRetrieve());
    Assert.assertEquals(2, result.size());
    // Two entities with IDs' id_1 and id_3 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on isRelatedTo");
      }
    }
  }
}
