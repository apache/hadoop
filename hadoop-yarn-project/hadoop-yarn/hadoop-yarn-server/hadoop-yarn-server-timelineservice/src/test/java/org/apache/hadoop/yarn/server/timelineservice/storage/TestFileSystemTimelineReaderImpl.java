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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileSystemTimelineReaderImpl {

  private static final String rootDir =
      FileSystemTimelineReaderImpl.DEFAULT_TIMELINE_SERVICE_STORAGE_DIR_ROOT;
  FileSystemTimelineReaderImpl reader;

  @BeforeClass
  public static void setup() throws Exception {
    loadEntityData();
    // Create app flow mapping file.
    CSVFormat format =
        CSVFormat.DEFAULT.withHeader("APP", "USER", "FLOW", "FLOWRUN");
    String appFlowMappingFile = rootDir + "/entities/cluster1/" +
        FileSystemTimelineReaderImpl.APP_FLOW_MAPPING_FILE;
    try (PrintWriter out =
        new PrintWriter(new BufferedWriter(
            new FileWriter(appFlowMappingFile, true)));
        CSVPrinter printer = new CSVPrinter(out, format)){
      printer.printRecord("app1", "user1", "flow1", 1);
      printer.printRecord("app2","user1","flow1,flow",1);
      printer.close();
    }
    (new File(rootDir)).deleteOnExit();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(rootDir));
  }

  @Before
  public void init() throws Exception {
    reader = new FileSystemTimelineReaderImpl();
    Configuration conf = new YarnConfiguration();
    conf.set(FileSystemTimelineReaderImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        rootDir);
    reader.init(conf);
  }

  private static void writeEntityFile(TimelineEntity entity, File dir)
      throws Exception {
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IOException("Could not create directories for " + dir);
      }
    }
    String fileName = dir.getAbsolutePath() + "/" + entity.getId() + ".thist";
    try (PrintWriter out =
        new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))){
      out.println(TimelineUtils.dumpTimelineRecordtoJSON(entity));
      out.write("\n");
      out.close();
    }
  }

  private static void loadEntityData() throws Exception {
    File appDir = new File(rootDir +
        "/entities/cluster1/user1/flow1/1/app1/app/");
    TimelineEntity entity11 = new TimelineEntity();
    entity11.setId("id_1");
    entity11.setType("app");
    entity11.setCreatedTime(1425016502000L);
    entity11.setModifiedTime(1425016502050L);
    Map<String, Object> info1 = new HashMap<String, Object>();
    info1.put("info1", "val1");
    entity11.addInfo(info1);
    TimelineEvent event = new TimelineEvent();
    event.setId("event_1");
    event.setTimestamp(1425016502003L);
    entity11.addEvent(event);
    Set<TimelineMetric> metrics = new HashSet<TimelineMetric>();
    TimelineMetric metric1 = new TimelineMetric();
    metric1.setId("metric1");
    metric1.setType(TimelineMetric.Type.SINGLE_VALUE);
    metric1.addValue(1425016502006L, 113.2F);
    metrics.add(metric1);
    TimelineMetric metric2 = new TimelineMetric();
    metric2.setId("metric2");
    metric2.setType(TimelineMetric.Type.TIME_SERIES);
    metric2.addValue(1425016502016L, 34);
    metrics.add(metric2);
    entity11.setMetrics(metrics);
    Map<String,String> configs = new HashMap<String, String>();
    configs.put("config_1", "123");
    entity11.setConfigs(configs);
    entity11.addRelatesToEntity("flow", "flow1");
    entity11.addIsRelatedToEntity("type1", "tid1_1");
    writeEntityFile(entity11, appDir);
    TimelineEntity entity12 = new TimelineEntity();
    entity12.setId("id_1");
    entity12.setType("app");
    entity12.setModifiedTime(1425016503000L);
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
    entity2.setModifiedTime(1425016502010L);
    Map<String, Object> info2 = new HashMap<String, Object>();
    info1.put("info2", 4);
    entity2.addInfo(info2);
    Map<String,String> configs2 = new HashMap<String, String>();
    configs2.put("config_1", "123");
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
    metric21.addValue(1425016501006L, 123.2F);
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
    entity3.setModifiedTime(1425016502010L);
    Map<String, Object> info3 = new HashMap<String, Object>();
    info3.put("info2", 3.5);
    entity3.addInfo(info3);
    Map<String,String> configs3 = new HashMap<String, String>();
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
    metric31.addValue(1425016501006L, 124.8F);
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
    entity4.setModifiedTime(1425016503010L);
    TimelineEvent event44 = new TimelineEvent();
    event44.setId("event_4");
    event44.setTimestamp(1425016502003L);
    entity4.addEvent(event44);
    writeEntityFile(entity4, appDir);

    File appDir2 = new File(rootDir +
            "/entities/cluster1/user1/flow1,flow/1/app2/app/");
    TimelineEntity entity5 = new TimelineEntity();
    entity5.setId("id_5");
    entity5.setType("app");
    entity5.setCreatedTime(1425016502050L);
    entity5.setModifiedTime(1425016503010L);
    writeEntityFile(entity5, appDir2);
  }

  public TimelineReader getTimelineReader() {
    return reader;
  }

  @Test
  public void testGetEntityDefaultView() throws Exception {
    // If no fields are specified, entity is returned with default view i.e.
    // only the id, created and modified time
    TimelineEntity result =
        reader.getEntity("user1", "cluster1", "flow1", 1L, "app1",
            "app", "id_1", null);
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals(1425016502000L, result.getCreatedTime());
    Assert.assertEquals(1425016503000L, result.getModifiedTime());
    Assert.assertEquals(0, result.getConfigs().size());
    Assert.assertEquals(0, result.getMetrics().size());
  }

  @Test
  public void testGetEntityByClusterAndApp() throws Exception {
    // Cluster and AppId should be enough to get an entity.
    TimelineEntity result =
        reader.getEntity(null, "cluster1", null, null, "app1",
            "app", "id_1", null);
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals(1425016502000L, result.getCreatedTime());
    Assert.assertEquals(1425016503000L, result.getModifiedTime());
    Assert.assertEquals(0, result.getConfigs().size());
    Assert.assertEquals(0, result.getMetrics().size());
  }

  /** This test checks whether we can handle commas in app flow mapping csv */
  @Test
  public void testAppFlowMappingCsv() throws Exception {
    // Test getting an entity by cluster and app where flow entry
    // in app flow mapping csv has commas.
    TimelineEntity result =
        reader.getEntity(null, "cluster1", null, null, "app2",
            "app", "id_5", null);
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_5")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals(1425016502050L, result.getCreatedTime());
    Assert.assertEquals(1425016503010L, result.getModifiedTime());
  }

  @Test
  public void testGetEntityCustomFields() throws Exception {
    // Specified fields in addition to default view will be returned.
    TimelineEntity result =
        reader.getEntity("user1", "cluster1", "flow1", 1L,
            "app1", "app", "id_1",
            EnumSet.of(Field.INFO, Field.CONFIGS, Field.METRICS));
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals(1425016502000L, result.getCreatedTime());
    Assert.assertEquals(1425016503000L, result.getModifiedTime());
    Assert.assertEquals(3, result.getConfigs().size());
    Assert.assertEquals(3, result.getMetrics().size());
    Assert.assertEquals(1, result.getInfo().size());
    // No events will be returned
    Assert.assertEquals(0, result.getEvents().size());
  }

  @Test
  public void testGetEntityAllFields() throws Exception {
    // All fields of TimelineEntity will be returned.
    TimelineEntity result =
        reader.getEntity("user1", "cluster1", "flow1", 1L,
            "app1", "app", "id_1", EnumSet.of(Field.ALL));
    Assert.assertEquals(
        (new TimelineEntity.Identifier("app", "id_1")).toString(),
        result.getIdentifier().toString());
    Assert.assertEquals(1425016502000L, result.getCreatedTime());
    Assert.assertEquals(1425016503000L, result.getModifiedTime());
    Assert.assertEquals(3, result.getConfigs().size());
    Assert.assertEquals(3, result.getMetrics().size());
    // All fields including events will be returned.
    Assert.assertEquals(2, result.getEvents().size());
  }

  @Test
  public void testGetAllEntities() throws Exception {
    Set<TimelineEntity> result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, null, null, null, null, null, null,
            null, null);
    // All 3 entities will be returned
    Assert.assertEquals(4, result.size());
  }

  @Test
  public void testGetEntitiesWithLimit() throws Exception {
    Set<TimelineEntity> result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            2L, null, null, null, null, null, null, null, null, null,
            null, null);
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
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            3L, null, null, null, null, null, null, null, null, null,
                null, null);
     // Even though 2 entities out of 4 have same created time, one entity
     // is left out due to limit
     Assert.assertEquals(3, result.size());
  }

  @Test
  public void testGetEntitiesByTimeWindows() throws Exception {
    // Get entities based on created time start and end time range.
    Set<TimelineEntity> result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, 1425016502030L, 1425016502060L, null, null, null, null, null,
            null, null, null, null);
    Assert.assertEquals(1, result.size());
    // Only one entity with ID id_4 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on created time range");
      }
    }

    // Get entities if only created time end is specified.
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, 1425016502010L, null, null, null, null, null, null,
            null, null, null);
    Assert.assertEquals(3, result.size());
    for (TimelineEntity entity : result) {
      if (entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on created time range");
      }
    }

    // Get entities if only created time start is specified.
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, 1425016502010L, null, null, null, null, null, null, null,
            null, null, null);
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on created time range");
      }
    }

    // Get entities based on modified time start and end time range.
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, 1425016502090L, 1425016503020L, null, null, null,
            null, null, null, null);
    Assert.assertEquals(2, result.size());
    // Two entities with IDs' id_1 and id_4 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on modified time range");
      }
    }

    // Get entities if only modified time end is specified.
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, 1425016502090L, null, null, null, null,
            null, null, null);
    Assert.assertEquals(2, result.size());
    for (TimelineEntity entity : result) {
      if (entity.getId().equals("id_1") || entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on modified time range");
      }
    }

    // Get entities if only modified time start is specified.
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, 1425016503005L, null, null, null, null, null,
            null, null, null);
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_4")) {
        Assert.fail("Incorrect filtering based on modified time range");
      }
    }
  }

  @Test
  public void testGetFilteredEntities() throws Exception {
    // Get entities based on info filters.
    Map<String, Object> infoFilters = new HashMap<String, Object>();
    infoFilters.put("info2", 3.5);
    Set<TimelineEntity> result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, null, null, null, infoFilters, null, null,
            null, null);
    Assert.assertEquals(1, result.size());
    // Only one entity with ID id_3 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on info filters");
      }
    }

    // Get entities based on config filters.
    Map<String, String> configFilters = new HashMap<String, String>();
    configFilters.put("config_1", "123");
    configFilters.put("config_3", "abc");
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, null, null, null, null, configFilters, null,
            null, null);
    Assert.assertEquals(2, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on config filters");
      }
    }

    // Get entities based on event filters.
    Set<String> eventFilters = new HashSet<String>();
    eventFilters.add("event_2");
    eventFilters.add("event_4");
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, null, null, null, null, null, null,
            eventFilters, null);
    Assert.assertEquals(1, result.size());
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on event filters");
      }
    }

    // Get entities based on metric filters.
    Set<String> metricFilters = new HashSet<String>();
    metricFilters.add("metric3");
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, null, null, null, null, null, metricFilters,
            null, null);
    Assert.assertEquals(2, result.size());
    // Two entities with IDs' id_1 and id_2 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_2")) {
        Assert.fail("Incorrect filtering based on metric filters");
      }
    }
  }

  @Test
  public void testGetEntitiesByRelations() throws Exception {
    // Get entities based on relatesTo.
    Map<String, Set<String>> relatesTo = new HashMap<String, Set<String>>();
    Set<String> relatesToIds = new HashSet<String>();
    relatesToIds.add("flow1");
    relatesTo.put("flow", relatesToIds);
    Set<TimelineEntity> result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, null, relatesTo, null, null, null, null,
            null, null);
    Assert.assertEquals(1, result.size());
    // Only one entity with ID id_1 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1")) {
        Assert.fail("Incorrect filtering based on relatesTo");
      }
    }

    // Get entities based on isRelatedTo.
    Map<String, Set<String>> isRelatedTo = new HashMap<String, Set<String>>();
    Set<String> isRelatedToIds = new HashSet<String>();
    isRelatedToIds.add("tid1_2");
    isRelatedTo.put("type1", isRelatedToIds);
    result =
        reader.getEntities("user1", "cluster1", "flow1", 1L, "app1", "app",
            null, null, null, null, null, null, isRelatedTo, null, null, null,
            null, null);
    Assert.assertEquals(2, result.size());
    // Two entities with IDs' id_1 and id_3 should be returned.
    for (TimelineEntity entity : result) {
      if (!entity.getId().equals("id_1") && !entity.getId().equals("id_3")) {
        Assert.fail("Incorrect filtering based on isRelatedTo");
      }
    }
  }
}
