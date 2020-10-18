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
import java.io.FileNotFoundException;
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
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  File System based implementation for TimelineReader. This implementation may
 *  not provide a complete implementation of all the necessary features. This
 *  implementation is provided solely for basic testing purposes, and should not
 *  be used in a non-test situation.
 */
public class FileSystemTimelineReaderImpl extends AbstractService
    implements TimelineReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemTimelineReaderImpl.class);

  private FileSystem fs;
  private Path rootPath;
  private Path entitiesPath;
  private static final String ENTITIES_DIR = "entities";

  /** Default extension for output files. */
  private static final String TIMELINE_SERVICE_STORAGE_EXTENSION = ".thist";

  @VisibleForTesting
  /** Default extension for output files. */
  static final String APP_FLOW_MAPPING_FILE = "app_flow_mapping.csv";

  /** Config param for timeline service file system storage root. */
  public static final String TIMELINE_SERVICE_STORAGE_DIR_ROOT =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "fs-writer.root-dir";

  /** Default value for storage location on local disk. */
  private static final String STORAGE_DIR_ROOT = "timeline_service_data";

  private final CSVFormat csvFormat =
      CSVFormat.DEFAULT.withHeader("APP", "USER", "FLOW", "FLOWRUN");

  public FileSystemTimelineReaderImpl() {
    super(FileSystemTimelineReaderImpl.class.getName());
  }

  @VisibleForTesting
  String getRootPath() {
    return rootPath.toString();
  }

  private static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
  }

  /**
   * Deserialize a POJO object from a JSON string.
   *
   * @param <T> Describes the type of class to be returned.
   * @param clazz class to be deserialized.
   * @param jsonString JSON string to deserialize.
   * @return An object based on class type. Used typically for
   *     <cite>TimelineEntity</cite> object.
   * @throws IOException if the underlying input source has problems during
   *     parsing.
   * @throws JsonMappingException  if parser has problems parsing content.
   * @throws JsonGenerationException if there is a problem in JSON writing.
   */
  public static <T> T getTimelineRecordFromJSON(
      String jsonString, Class<T> clazz)
      throws JsonGenerationException, JsonMappingException, IOException {
    return mapper.readValue(jsonString, clazz);
  }

  private static void fillFields(TimelineEntity finalEntity,
      TimelineEntity real, EnumSet<Field> fields) {
    if (fields.contains(Field.ALL)) {
      fields = EnumSet.allOf(Field.class);
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

  private String getFlowRunPath(String userId, String clusterId,
      String flowName, Long flowRunId, String appId) throws IOException {
    if (userId != null && flowName != null && flowRunId != null) {
      return userId + File.separator + flowName + File.separator + flowRunId;
    }
    if (clusterId == null || appId == null) {
      throw new IOException("Unable to get flow info");
    }
    Path clusterIdPath = new Path(entitiesPath, clusterId);
    Path appFlowMappingFilePath = new Path(clusterIdPath,
            APP_FLOW_MAPPING_FILE);
    try (BufferedReader reader =
             new BufferedReader(new InputStreamReader(
                 fs.open(appFlowMappingFilePath), Charset.forName("UTF-8")));
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
        return record.get(1).trim() + File.separator + record.get(2).trim() +
            File.separator + record.get(3).trim();
      }
      parser.close();
    }
    throw new IOException("Unable to get flow info");
  }

  private static TimelineEntity createEntityToBeReturned(TimelineEntity entity,
      EnumSet<Field> fieldsToRetrieve) {
    TimelineEntity entityToBeReturned = new TimelineEntity();
    entityToBeReturned.setIdentifier(entity.getIdentifier());
    entityToBeReturned.setCreatedTime(entity.getCreatedTime());
    if (fieldsToRetrieve != null) {
      fillFields(entityToBeReturned, entity, fieldsToRetrieve);
    }
    return entityToBeReturned;
  }

  private static boolean isTimeInRange(Long time, Long timeBegin,
      Long timeEnd) {
    return (time >= timeBegin) && (time <= timeEnd);
  }

  private static void mergeEntities(TimelineEntity entity1,
      TimelineEntity entity2) {
    // Ideally created time wont change except in the case of issue from client.
    if (entity2.getCreatedTime() != null && entity2.getCreatedTime() > 0) {
      entity1.setCreatedTime(entity2.getCreatedTime());
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

  private Set<TimelineEntity> getEntities(Path dir, String entityType,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
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
    if (dir != null) {
      RemoteIterator<LocatedFileStatus> fileStatuses = fs.listFiles(dir,
              false);
      if (fileStatuses != null) {
        while (fileStatuses.hasNext()) {
          LocatedFileStatus locatedFileStatus = fileStatuses.next();
          Path entityFile = locatedFileStatus.getPath();
          if (!entityFile.getName()
              .contains(TIMELINE_SERVICE_STORAGE_EXTENSION)) {
            continue;
          }
          try (BufferedReader reader = new BufferedReader(
              new InputStreamReader(fs.open(entityFile),
                  Charset.forName("UTF-8")))) {
            TimelineEntity entity = readEntityFromFile(reader);
            if (!entity.getType().equals(entityType)) {
              continue;
            }
            if (!isTimeInRange(entity.getCreatedTime(),
                filters.getCreatedTimeBegin(),
                filters.getCreatedTimeEnd())) {
              continue;
            }
            if (filters.getRelatesTo() != null &&
                !filters.getRelatesTo().getFilterList().isEmpty() &&
                !TimelineStorageUtils.matchRelatesTo(entity,
                    filters.getRelatesTo())) {
              continue;
            }
            if (filters.getIsRelatedTo() != null &&
                !filters.getIsRelatedTo().getFilterList().isEmpty() &&
                !TimelineStorageUtils.matchIsRelatedTo(entity,
                    filters.getIsRelatedTo())) {
              continue;
            }
            if (filters.getInfoFilters() != null &&
                !filters.getInfoFilters().getFilterList().isEmpty() &&
                !TimelineStorageUtils.matchInfoFilters(entity,
                    filters.getInfoFilters())) {
              continue;
            }
            if (filters.getConfigFilters() != null &&
                !filters.getConfigFilters().getFilterList().isEmpty() &&
                !TimelineStorageUtils.matchConfigFilters(entity,
                    filters.getConfigFilters())) {
              continue;
            }
            if (filters.getMetricFilters() != null &&
                !filters.getMetricFilters().getFilterList().isEmpty() &&
                !TimelineStorageUtils.matchMetricFilters(entity,
                    filters.getMetricFilters())) {
              continue;
            }
            if (filters.getEventFilters() != null &&
                !filters.getEventFilters().getFilterList().isEmpty() &&
                !TimelineStorageUtils.matchEventFilters(entity,
                    filters.getEventFilters())) {
              continue;
            }
            TimelineEntity entityToBeReturned = createEntityToBeReturned(
                entity, dataToRetrieve.getFieldsToRetrieve());
            Set<TimelineEntity> entitiesCreatedAtSameTime =
                sortedEntities.get(entityToBeReturned.getCreatedTime());
            if (entitiesCreatedAtSameTime == null) {
              entitiesCreatedAtSameTime = new HashSet<TimelineEntity>();
            }
            entitiesCreatedAtSameTime.add(entityToBeReturned);
            sortedEntities.put(entityToBeReturned.getCreatedTime(),
                entitiesCreatedAtSameTime);
          }
        }
      }
    }

    Set<TimelineEntity> entities = new HashSet<TimelineEntity>();
    long entitiesAdded = 0;
    for (Set<TimelineEntity> entitySet : sortedEntities.values()) {
      for (TimelineEntity entity : entitySet) {
        entities.add(entity);
        ++entitiesAdded;
        if (entitiesAdded >= filters.getLimit()) {
          return entities;
        }
      }
    }
    return entities;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    String outputRoot = conf.get(TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        conf.get("hadoop.tmp.dir") + File.separator + STORAGE_DIR_ROOT);
    rootPath = new Path(outputRoot);
    entitiesPath = new Path(rootPath, ENTITIES_DIR);
    fs = rootPath.getFileSystem(conf);
    super.serviceInit(conf);
  }

  @Override
  public TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException {
    String flowRunPathStr = getFlowRunPath(context.getUserId(),
        context.getClusterId(), context.getFlowName(), context.getFlowRunId(),
        context.getAppId());
    Path clusterIdPath = new Path(entitiesPath, context.getClusterId());
    Path flowRunPath = new Path(clusterIdPath, flowRunPathStr);
    Path appIdPath = new Path(flowRunPath, context.getAppId());
    Path entityTypePath = new Path(appIdPath, context.getEntityType());
    Path entityFilePath = new Path(entityTypePath,
            context.getEntityId() + TIMELINE_SERVICE_STORAGE_EXTENSION);

    try (BufferedReader reader =
             new BufferedReader(new InputStreamReader(
                 fs.open(entityFilePath), Charset.forName("UTF-8")))) {
      TimelineEntity entity = readEntityFromFile(reader);
      return createEntityToBeReturned(
          entity, dataToRetrieve.getFieldsToRetrieve());
    } catch (FileNotFoundException e) {
      LOG.info("Cannot find entity {id:" + context.getEntityId() + " , type:" +
          context.getEntityType() + "}. Will send HTTP 404 in response.");
      return null;
    }
  }

  @Override
  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
    String flowRunPathStr = getFlowRunPath(context.getUserId(),
        context.getClusterId(), context.getFlowName(), context.getFlowRunId(),
        context.getAppId());
    Path clusterIdPath = new Path(entitiesPath, context.getClusterId());
    Path flowRunPath = new Path(clusterIdPath, flowRunPathStr);
    Path appIdPath = new Path(flowRunPath, context.getAppId());
    Path entityTypePath = new Path(appIdPath, context.getEntityType());

    return getEntities(entityTypePath, context.getEntityType(), filters,
            dataToRetrieve);
  }

  @Override public Set<String> getEntityTypes(TimelineReaderContext context)
      throws IOException {
    Set<String> result = new TreeSet<>();
    String flowRunPathStr = getFlowRunPath(context.getUserId(),
        context.getClusterId(), context.getFlowName(), context.getFlowRunId(),
        context.getAppId());
    if (context.getUserId() == null) {
      context.setUserId(new Path(flowRunPathStr).getParent().getParent().
          getName());
    }
    Path clusterIdPath = new Path(entitiesPath, context.getClusterId());
    Path flowRunPath = new Path(clusterIdPath, flowRunPathStr);
    Path appIdPath = new Path(flowRunPath, context.getAppId());
    FileStatus[] fileStatuses = fs.listStatus(appIdPath);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        result.add(fileStatus.getPath().getName());
      }
    }
    return result;
  }

  @Override
  public TimelineHealth getHealthStatus() {
    try {
      fs.exists(rootPath);
    } catch (IOException e) {
      return new TimelineHealth(
          TimelineHealth.TimelineHealthStatus.READER_CONNECTION_FAILURE,
          e.getMessage()
          );
    }
    return new TimelineHealth(TimelineHealth.TimelineHealthStatus.RUNNING,
        "");
  }
}