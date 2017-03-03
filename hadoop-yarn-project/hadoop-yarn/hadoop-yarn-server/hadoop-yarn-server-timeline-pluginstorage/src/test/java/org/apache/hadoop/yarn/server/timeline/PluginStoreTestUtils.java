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
package org.apache.hadoop.yarn.server.timeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Utility methods related to the ATS v1.5 plugin storage tests.
 */
public class PluginStoreTestUtils {

  /**
   * For a given file system, setup directories ready to test the plugin storage.
   *
   * @param fs a {@link FileSystem} object that the plugin storage will work with
   * @return the dfsCluster ready to start plugin storage tests.
   * @throws IOException
   */
  public static FileSystem prepareFileSystemForPluginStore(FileSystem fs)
      throws IOException {
    Path activeDir = new Path(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR_DEFAULT
    );
    Path doneDir = new Path(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR_DEFAULT
    );

    fs.mkdirs(activeDir);
    fs.mkdirs(doneDir);
    return fs;
  }

  /**
   * Prepare configuration for plugin tests. This method will also add the mini
   * DFS cluster's info to the configuration.
   * Note: the test program needs to setup the reader plugin by itself.
   *
   * @param conf
   * @param dfsCluster
   * @return the modified configuration
   */
  public static YarnConfiguration prepareConfiguration(YarnConfiguration conf,
      MiniDFSCluster dfsCluster) {
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        dfsCluster.getURI().toString());
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.5f);
    conf.setLong(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS,
        1);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_STORE,
        EntityGroupFSTimelineStore.class.getName());
    return conf;
  }

  static FSDataOutputStream createLogFile(Path logPath, FileSystem fs)
      throws IOException {
    FSDataOutputStream stream;
    stream = fs.create(logPath, true);
    return stream;
  }

  static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setAnnotationIntrospector(
        new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return mapper;
  }

  /**
   * Create sample entities for testing
   * @return two timeline entities in a {@link TimelineEntities} object
   */
  static TimelineEntities generateTestEntities() {
    TimelineEntities entities = new TimelineEntities();
    Map<String, Set<Object>> primaryFilters =
        new HashMap<String, Set<Object>>();
    Set<Object> l1 = new HashSet<Object>();
    l1.add("username");
    Set<Object> l2 = new HashSet<Object>();
    l2.add(Integer.MAX_VALUE);
    Set<Object> l3 = new HashSet<Object>();
    l3.add("123abc");
    Set<Object> l4 = new HashSet<Object>();
    l4.add((long)Integer.MAX_VALUE + 1l);
    primaryFilters.put("user", l1);
    primaryFilters.put("appname", l2);
    primaryFilters.put("other", l3);
    primaryFilters.put("long", l4);
    Map<String, Object> secondaryFilters = new HashMap<String, Object>();
    secondaryFilters.put("startTime", 123456);
    secondaryFilters.put("status", "RUNNING");
    Map<String, Object> otherInfo1 = new HashMap<String, Object>();
    otherInfo1.put("info1", "val1");
    otherInfo1.putAll(secondaryFilters);

    String entityId1 = "id_1";
    String entityType1 = "type_1";
    String entityId2 = "id_2";
    String entityType2 = "type_2";

    Map<String, Set<String>> relatedEntities =
        new HashMap<String, Set<String>>();
    relatedEntities.put(entityType2, Collections.singleton(entityId2));

    TimelineEvent ev3 = createEvent(789l, "launch_event", null);
    TimelineEvent ev4 = createEvent(0l, "init_event", null);
    List<TimelineEvent> events = new ArrayList<TimelineEvent>();
    events.add(ev3);
    events.add(ev4);
    entities.addEntity(createEntity(entityId2, entityType2, 456l, events, null,
        null, null, "domain_id_1"));

    TimelineEvent ev1 = createEvent(123l, "start_event", null);
    entities.addEntity(createEntity(entityId1, entityType1, 123l,
        Collections.singletonList(ev1), relatedEntities, primaryFilters,
        otherInfo1, "domain_id_1"));
    return entities;
  }

  static void verifyTestEntities(TimelineDataManager tdm)
      throws YarnException, IOException {
    TimelineEntity entity1 = tdm.getEntity("type_1", "id_1",
        EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    TimelineEntity entity2 = tdm.getEntity("type_2", "id_2",
        EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertNotNull(entity1);
    assertNotNull(entity2);
    assertEquals("Failed to read out entity 1",
        (Long) 123l, entity1.getStartTime());
    assertEquals("Failed to read out entity 2",
        (Long) 456l, entity2.getStartTime());
  }

  /**
   * Create a test entity
   */
  static TimelineEntity createEntity(String entityId, String entityType,
      Long startTime, List<TimelineEvent> events,
      Map<String, Set<String>> relatedEntities,
      Map<String, Set<Object>> primaryFilters,
      Map<String, Object> otherInfo, String domainId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(entityId);
    entity.setEntityType(entityType);
    entity.setStartTime(startTime);
    entity.setEvents(events);
    if (relatedEntities != null) {
      for (Map.Entry<String, Set<String>> e : relatedEntities.entrySet()) {
        for (String v : e.getValue()) {
          entity.addRelatedEntity(e.getKey(), v);
        }
      }
    } else {
      entity.setRelatedEntities(null);
    }
    entity.setPrimaryFilters(primaryFilters);
    entity.setOtherInfo(otherInfo);
    entity.setDomainId(domainId);
    return entity;
  }

  /**
   * Create a test event
   */
  static TimelineEvent createEvent(long timestamp, String type, Map<String,
      Object> info) {
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(timestamp);
    event.setEventType(type);
    event.setEventInfo(info);
    return event;
  }

  /**
   * Write timeline entities to a file system
   * @param entities
   * @param logPath
   * @param fs
   * @throws IOException
   */
  static void writeEntities(TimelineEntities entities, Path logPath,
      FileSystem fs) throws IOException {
    FSDataOutputStream outStream = createLogFile(logPath, fs);
    JsonGenerator jsonGenerator
        = new JsonFactory().createGenerator(outStream);
    jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
    ObjectMapper objMapper = createObjectMapper();
    for (TimelineEntity entity : entities.getEntities()) {
      objMapper.writeValue(jsonGenerator, entity);
    }
    outStream.close();
  }

  static TimelineDataManager getTdmWithStore(Configuration config, TimelineStore store) {
    TimelineACLsManager aclManager = new TimelineACLsManager(config);
    TimelineDataManager tdm = new TimelineDataManager(store, aclManager);
    tdm.init(config);
    return tdm;
  }

  static TimelineDataManager getTdmWithMemStore(Configuration config) {
    TimelineStore store = new MemoryTimelineStore("MemoryStore.test");
    TimelineDataManager tdm = getTdmWithStore(config, store);
    return tdm;
  }

}
