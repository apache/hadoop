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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;

/**
 *  File System based implementation for TimelineReader.
 */
public class FileSystemTimelineReaderImpl extends AbstractService
    implements TimelineReader {

  private static final Log LOG =
      LogFactory.getLog(FileSystemTimelineReaderImpl.class);

  private String rootPath;
  private static final String ENTITIES_DIR = "entities";

  /** Default extension for output files. */
  private static final String TIMELINE_SERVICE_STORAGE_EXTENSION = ".thist";

  @VisibleForTesting
  /** Default extension for output files. */
  static final String APP_FLOW_MAPPING_FILE = "app_flow_mapping.csv";

  @VisibleForTesting
  /** Config param for timeline service file system storage root. */
  static final String TIMELINE_SERVICE_STORAGE_DIR_ROOT =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "fs-writer.root-dir";

  @VisibleForTesting
  /** Default value for storage location on local disk. */
  static final String DEFAULT_TIMELINE_SERVICE_STORAGE_DIR_ROOT =
      "/tmp/timeline_service_data";

  private final CSVFormat csvFormat =
      CSVFormat.DEFAULT.withHeader("APP", "USER", "FLOW", "FLOWRUN");

  public FileSystemTimelineReaderImpl() {
    super(FileSystemTimelineReaderImpl.class.getName());
  }

  @VisibleForTesting
  String getRootPath() {
    return rootPath;
  }

  private static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
  }

  /**
   * Deserialize a POJO object from a JSON string.
   * @param clazz
   *      class to be desirialized
   *
   * @param jsonString
   *    json string to deserialize
   * @return TimelineEntity object
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonGenerationException
   */
  public static <T> T getTimelineRecordFromJSON(
      String jsonString, Class<T> clazz)
      throws JsonGenerationException, JsonMappingException, IOException {
    return mapper.readValue(jsonString, clazz);
  }

  private static void fillFields(TimelineEntity finalEntity,
      TimelineEntity real, EnumSet<Field> fields) {
    if (fields.contains(Field.ALL)) {
      finalEntity.setConfigs(real.getConfigs());
      finalEntity.setMetrics(real.getMetrics());
      finalEntity.setInfo(real.getInfo());
      finalEntity.setIsRelatedToEntities(real.getIsRelatedToEntities());
      finalEntity.setIsRelatedToEntities(real.getIsRelatedToEntities());
      finalEntity.setEvents(real.getEvents());
      return;
    }
    for (Field field : fields) {
      switch(field) {
      case CONFIGS:
        finalEntity.setConfigs(real.getConfigs());
        break;
      case METRICS:
        finalEntity.setMetrics(real.getMetrics());
        break;
      case INFO:
        finalEntity.setInfo(real.getInfo());
        break;
      case IS_RELATED_TO:
        finalEntity.setIsRelatedToEntities(real.getIsRelatedToEntities());
        break;
      case RELATES_TO:
        finalEntity.setIsRelatedToEntities(real.getIsRelatedToEntities());
        break;
      case EVENTS:
        finalEntity.setEvents(real.getEvents());
        break;
      default:
        continue;
      }
    }
  }

  private static boolean matchFilter(Object infoValue, Object filterValue) {
    return infoValue.equals(filterValue);
  }

  private static boolean matchFilters(Map<String, ? extends Object> entityInfo,
      Map<String, ? extends Object> filters) {
    if (entityInfo == null || entityInfo.isEmpty()) {
      return false;
    }
    for (Map.Entry<String, ? extends Object> filter : filters.entrySet()) {
      Object infoValue = entityInfo.get(filter.getKey());
      if (infoValue == null) {
        return false;
      }
      if (!matchFilter(infoValue, filter.getValue())) {
        return false;
      }
    }
    return true;
  }

  private String getFlowRunPath(String userId, String clusterId, String flowId,
      Long flowRunId, String appId)
      throws IOException {
    if (userId != null && flowId != null && flowRunId != null) {
      return userId + "/" + flowId + "/" + flowRunId;
    }
    if (clusterId == null || appId == null) {
      throw new IOException("Unable to get flow info");
    }
    String appFlowMappingFile = rootPath + "/" +  ENTITIES_DIR + "/" +
        clusterId + "/" + APP_FLOW_MAPPING_FILE;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(
            new FileInputStream(
                appFlowMappingFile), Charset.forName("UTF-8")));
        CSVParser parser = new CSVParser(reader, csvFormat)) {
      for (CSVRecord record : parser.getRecords()) {
        if (record.size() < 4) {
          continue;
        }
        String applicationId = record.get("APP");
        if (applicationId != null && !applicationId.trim().isEmpty() &&
            !applicationId.trim().equals(appId)) {
          continue;
        }
        return record.get(1).trim() + "/" + record.get(2).trim() + "/" +
            record.get(3).trim();
      }
      parser.close();
    }
    throw new IOException("Unable to get flow info");
  }

  private static boolean matchMetricFilters(Set<TimelineMetric> metrics,
      Set<String> metricFilters) {
    Set<String> tempMetrics = new HashSet<String>();
    for (TimelineMetric metric : metrics) {
      tempMetrics.add(metric.getId());
    }

    for (String metricFilter : metricFilters) {
      if (!tempMetrics.contains(metricFilter)) {
        return false;
      }
    }
    return true;
  }

  private static boolean matchEventFilters(Set<TimelineEvent> entityEvents,
      Set<String> eventFilters) {
    Set<String> tempEvents = new HashSet<String>();
    for (TimelineEvent event : entityEvents) {
      tempEvents.add(event.getId());
    }

    for (String eventFilter : eventFilters) {
      if (!tempEvents.contains(eventFilter)) {
        return false;
      }
    }
    return true;
  }

  private static TimelineEntity createEntityToBeReturned(TimelineEntity entity,
      EnumSet<Field> fieldsToRetrieve) {
    TimelineEntity entityToBeReturned = new TimelineEntity();
    entityToBeReturned.setIdentifier(entity.getIdentifier());
    entityToBeReturned.setCreatedTime(entity.getCreatedTime());
    entityToBeReturned.setModifiedTime(entity.getModifiedTime());
    if (fieldsToRetrieve != null) {
      fillFields(entityToBeReturned, entity, fieldsToRetrieve);
    }
    return entityToBeReturned;
  }

  private static boolean isTimeInRange(Long time, Long timeBegin,
      Long timeEnd) {
    return (time >= timeBegin) && (time <= timeEnd);
  }

  private static boolean matchRelations(
      Map<String, Set<String>> entityRelations,
      Map<String, Set<String>> relations) {
    for (Map.Entry<String, Set<String>> relation : relations.entrySet()) {
      Set<String> ids = entityRelations.get(relation.getKey());
      if (ids == null) {
        return false;
      }
      for (String id : relation.getValue()) {
        if (!ids.contains(id)) {
          return false;
        }
      }
    }
    return true;
  }

  private static void mergeEntities(TimelineEntity entity1,
      TimelineEntity entity2) {
    // Ideally created time wont change except in the case of issue from client.
    if (entity2.getCreatedTime() > 0) {
      entity1.setCreatedTime(entity2.getCreatedTime());
    }
    if (entity2.getModifiedTime() > 0) {
      entity1.setModifiedTime(entity2.getModifiedTime());
    }
    for (Entry<String, String> configEntry : entity2.getConfigs().entrySet()) {
      entity1.addConfig(configEntry.getKey(), configEntry.getValue());
    }
    for (Entry<String, Object> infoEntry : entity2.getInfo().entrySet()) {
      entity1.addInfo(infoEntry.getKey(), infoEntry.getValue());
    }
    for (Entry<String, Set<String>> isRelatedToEntry :
        entity2.getIsRelatedToEntities().entrySet()) {
      String type = isRelatedToEntry.getKey();
      for (String entityId : isRelatedToEntry.getValue()) {
        entity1.addIsRelatedToEntity(type, entityId);
      }
    }
    for (Entry<String, Set<String>> relatesToEntry :
        entity2.getRelatesToEntities().entrySet()) {
      String type = relatesToEntry.getKey();
      for (String entityId : relatesToEntry.getValue()) {
        entity1.addRelatesToEntity(type, entityId);
      }
    }
    for (TimelineEvent event : entity2.getEvents()) {
      entity1.addEvent(event);
    }
    for (TimelineMetric metric2 : entity2.getMetrics()) {
      boolean found = false;
      for (TimelineMetric metric1 : entity1.getMetrics()) {
        if (metric1.getId().equals(metric2.getId())) {
          metric1.addValues(metric2.getValues());
          found = true;
          break;
        }
      }
      if (!found) {
        entity1.addMetric(metric2);
      }
    }
  }

  private static TimelineEntity readEntityFromFile(BufferedReader reader)
      throws IOException {
    TimelineEntity entity =
        getTimelineRecordFromJSON(reader.readLine(), TimelineEntity.class);
    String entityStr = "";
    while ((entityStr = reader.readLine()) != null) {
      if (entityStr.trim().isEmpty()) {
        continue;
      }
      TimelineEntity anotherEntity =
          getTimelineRecordFromJSON(entityStr, TimelineEntity.class);
      if (!entity.getId().equals(anotherEntity.getId()) ||
          !entity.getType().equals(anotherEntity.getType())) {
        continue;
      }
      mergeEntities(entity, anotherEntity);
    }
    return entity;
  }

  private Set<TimelineEntity> getEntities(File dir, String entityType,
      Long limit, Long createdTimeBegin,
      Long createdTimeEnd, Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    if (limit == null || limit <= 0) {
      limit = DEFAULT_LIMIT;
    }
    if (createdTimeBegin == null || createdTimeBegin <= 0) {
      createdTimeBegin = 0L;
    }
    if (createdTimeEnd == null || createdTimeEnd <= 0) {
      createdTimeEnd = Long.MAX_VALUE;
    }
    if (modifiedTimeBegin == null || modifiedTimeBegin <= 0) {
      modifiedTimeBegin = 0L;
    }
    if (modifiedTimeEnd == null || modifiedTimeEnd <= 0) {
      modifiedTimeEnd = Long.MAX_VALUE;
    }

    // First sort the selected entities based on created/start time.
    Map<Long, Set<TimelineEntity>> sortedEntities =
        new TreeMap<>(
          new Comparator<Long>() {
            @Override
            public int compare(Long l1, Long l2) {
              return l2.compareTo(l1);
            }
          }
        );
    for (File entityFile : dir.listFiles()) {
      if (!entityFile.getName().contains(TIMELINE_SERVICE_STORAGE_EXTENSION)) {
        continue;
      }
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  new FileInputStream(
                      entityFile), Charset.forName("UTF-8")))) {
        TimelineEntity entity = readEntityFromFile(reader);
        if (!entity.getType().equals(entityType)) {
          continue;
        }
        if (!isTimeInRange(entity.getCreatedTime(), createdTimeBegin,
            createdTimeEnd)) {
          continue;
        }
        if (!isTimeInRange(entity.getModifiedTime(), modifiedTimeBegin,
            modifiedTimeEnd)) {
          continue;
        }
        if (relatesTo != null && !relatesTo.isEmpty() &&
            !matchRelations(entity.getRelatesToEntities(), relatesTo)) {
          continue;
        }
        if (isRelatedTo != null && !isRelatedTo.isEmpty() &&
            !matchRelations(entity.getIsRelatedToEntities(), isRelatedTo)) {
          continue;
        }
        if (infoFilters != null && !infoFilters.isEmpty() &&
            !matchFilters(entity.getInfo(), infoFilters)) {
          continue;
        }
        if (configFilters != null && !configFilters.isEmpty() &&
            !matchFilters(entity.getConfigs(), configFilters)) {
          continue;
        }
        if (metricFilters != null && !metricFilters.isEmpty() &&
            !matchMetricFilters(entity.getMetrics(), metricFilters)) {
          continue;
        }
        if (eventFilters != null && !eventFilters.isEmpty() &&
            !matchEventFilters(entity.getEvents(), eventFilters)) {
          continue;
        }
        TimelineEntity entityToBeReturned =
            createEntityToBeReturned(entity, fieldsToRetrieve);
        Set<TimelineEntity> entitiesCreatedAtSameTime =
            sortedEntities.get(entityToBeReturned.getCreatedTime());
        if (entitiesCreatedAtSameTime == null) {
          entitiesCreatedAtSameTime = new HashSet<TimelineEntity>();
        }
        entitiesCreatedAtSameTime.add(entityToBeReturned);
        sortedEntities.put(
            entityToBeReturned.getCreatedTime(), entitiesCreatedAtSameTime);
      }
    }

    Set<TimelineEntity> entities = new HashSet<TimelineEntity>();
    long entitiesAdded = 0;
    for (Set<TimelineEntity> entitySet : sortedEntities.values()) {
      for (TimelineEntity entity : entitySet) {
        entities.add(entity);
        ++entitiesAdded;
        if (entitiesAdded >= limit) {
          return entities;
        }
      }
    }
    return entities;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    rootPath = conf.get(TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        DEFAULT_TIMELINE_SERVICE_STORAGE_DIR_ROOT);
    super.serviceInit(conf);
  }

  @Override
  public TimelineEntity getEntity(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      String entityId, EnumSet<Field> fieldsToRetrieve) throws IOException {
    String flowRunPath = getFlowRunPath(userId, clusterId, flowId,
        flowRunId, appId);
    File dir = new File(new File(rootPath, ENTITIES_DIR),
        clusterId + "/" + flowRunPath + "/" + appId + "/" + entityType);
    File entityFile =
        new File(dir, entityId + TIMELINE_SERVICE_STORAGE_EXTENSION);
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(
            new FileInputStream(entityFile), Charset.forName("UTF-8")))) {
      TimelineEntity entity = readEntityFromFile(reader);
      return createEntityToBeReturned(entity, fieldsToRetrieve);
    }
  }

  @Override
  public Set<TimelineEntity> getEntities(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    String flowRunPath =
        getFlowRunPath(userId, clusterId, flowId, flowRunId, appId);
    File dir =
        new File(new File(rootPath, ENTITIES_DIR),
            clusterId + "/" + flowRunPath + "/" + appId + "/" + entityType);
    return getEntities(dir, entityType, limit,
        createdTimeBegin, createdTimeEnd, modifiedTimeBegin, modifiedTimeEnd,
        relatesTo, isRelatedTo, infoFilters, configFilters, metricFilters,
        eventFilters, fieldsToRetrieve);
  }
}