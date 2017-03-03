/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.timeline;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

abstract class LogInfo {
  public static final String ENTITY_FILE_NAME_DELIMITERS = "_.";

  public String getAttemptDirName() {
    return attemptDirName;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long newOffset) {
    this.offset = newOffset;
  }

  private String attemptDirName;
  private String filename;
  private String user;
  private long offset = 0;

  private static final Logger LOG = LoggerFactory.getLogger(LogInfo.class);

  public LogInfo(String attemptDirName, String file, String owner) {
    this.attemptDirName = attemptDirName;
    filename = file;
    user = owner;
  }

  public Path getPath(Path rootPath) {
    Path attemptPath = new Path(rootPath, attemptDirName);
    return new Path(attemptPath, filename);
  }

  public String getFilename() {
    return filename;
  }

  public boolean matchesGroupId(TimelineEntityGroupId groupId) {
    return matchesGroupId(groupId.toString());
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  boolean matchesGroupId(String groupId){
    // Return true if the group id is a segment (separated by _, ., or end of
    // string) of the file name.
    int pos = filename.indexOf(groupId);
    if (pos < 0) {
      return false;
    }
    return filename.length() == pos + groupId.length()
        || ENTITY_FILE_NAME_DELIMITERS.contains(String.valueOf(
        filename.charAt(pos + groupId.length())
    ));
  }

  public long parseForStore(TimelineDataManager tdm, Path appDirPath,
      boolean appCompleted, JsonFactory jsonFactory, ObjectMapper objMapper,
      FileSystem fs) throws IOException {
    LOG.debug("Parsing for log dir {} on attempt {}", appDirPath,
        attemptDirName);
    Path logPath = getPath(appDirPath);
    FileStatus status = fs.getFileStatus(logPath);
    long numParsed = 0;
    if (status != null) {
      long startTime = Time.monotonicNow();
      try {
        LOG.debug("Parsing {} at offset {}", logPath, offset);
        long count = parsePath(tdm, logPath, appCompleted, jsonFactory,
            objMapper, fs);
        LOG.info("Parsed {} entities from {} in {} msec",
            count, logPath, Time.monotonicNow() - startTime);
        numParsed += count;
      } catch (RuntimeException e) {
        // If AppLogs cannot parse this log, it may be corrupted or just empty
        if (e.getCause() instanceof JsonParseException &&
            (status.getLen() > 0 || offset > 0)) {
          // log on parse problems if the file as been read in the past or
          // is visibly non-empty
          LOG.info("Log {} appears to be corrupted. Skip. ", logPath);
        }
      }
    } else {
      LOG.warn("{} no longer exists. Skip for scanning. ", logPath);
    }
    return numParsed;
  }

  private long parsePath(TimelineDataManager tdm, Path logPath,
      boolean appCompleted, JsonFactory jsonFactory, ObjectMapper objMapper,
      FileSystem fs) throws IOException {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(user);
    FSDataInputStream in = fs.open(logPath);
    JsonParser parser = null;
    try {
      in.seek(offset);
      try {
        parser = jsonFactory.createParser(in);
        parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
      } catch (IOException e) {
        // if app hasn't completed then there may be errors due to the
        // incomplete file which are treated as EOF until app completes
        if (appCompleted) {
          throw e;
        } else {
          LOG.debug("Exception in parse path: {}", e.getMessage());
          return 0;
        }
      }

      return doParse(tdm, parser, objMapper, ugi, appCompleted);
    } finally {
      IOUtils.closeStream(parser);
      IOUtils.closeStream(in);
    }
  }

  protected abstract long doParse(TimelineDataManager tdm, JsonParser parser,
      ObjectMapper objMapper, UserGroupInformation ugi, boolean appCompleted)
      throws IOException;
}

class EntityLogInfo extends LogInfo {
  private static final Logger LOG = LoggerFactory.getLogger(
      EntityGroupFSTimelineStore.class);

  public EntityLogInfo(String attemptId,
      String file, String owner) {
    super(attemptId, file, owner);
  }

  @Override
  protected long doParse(TimelineDataManager tdm, JsonParser parser,
      ObjectMapper objMapper, UserGroupInformation ugi, boolean appCompleted)
      throws IOException {
    long count = 0;
    TimelineEntities entities = new TimelineEntities();
    ArrayList<TimelineEntity> entityList = new ArrayList<TimelineEntity>(1);
    long bytesParsed;
    long bytesParsedLastBatch = 0;
    boolean postError = false;
    try {
      MappingIterator<TimelineEntity> iter = objMapper.readValues(parser,
          TimelineEntity.class);

      while (iter.hasNext()) {
        TimelineEntity entity = iter.next();
        String etype = entity.getEntityType();
        String eid = entity.getEntityId();
        LOG.trace("Read entity {}", etype);
        ++count;
        bytesParsed = parser.getCurrentLocation().getCharOffset() + 1;
        LOG.trace("Parser now at offset {}", bytesParsed);

        try {
          LOG.debug("Adding {}({}) to store", eid, etype);
          entityList.add(entity);
          entities.setEntities(entityList);
          TimelinePutResponse response = tdm.postEntities(entities, ugi);
          for (TimelinePutResponse.TimelinePutError e
              : response.getErrors()) {
            LOG.warn("Error putting entity: {} ({}): {}",
                e.getEntityId(), e.getEntityType(), e.getErrorCode());
          }
          setOffset(getOffset() + bytesParsed - bytesParsedLastBatch);
          bytesParsedLastBatch = bytesParsed;
          entityList.clear();
        } catch (YarnException e) {
          postError = true;
          throw new IOException("Error posting entities", e);
        } catch (IOException e) {
          postError = true;
          throw new IOException("Error posting entities", e);
        }
      }
    } catch (IOException e) {
      // if app hasn't completed then there may be errors due to the
      // incomplete file which are treated as EOF until app completes
      if (appCompleted || postError) {
        throw e;
      }
    } catch (RuntimeException e) {
      if (appCompleted || !(e.getCause() instanceof JsonParseException)) {
        throw e;
      }
    }
    return count;
  }
}

class DomainLogInfo extends LogInfo {
  private static final Logger LOG = LoggerFactory.getLogger(
      EntityGroupFSTimelineStore.class);

  public DomainLogInfo(String attemptDirName, String file,
      String owner) {
    super(attemptDirName, file, owner);
  }

  protected long doParse(TimelineDataManager tdm, JsonParser parser,
      ObjectMapper objMapper, UserGroupInformation ugi, boolean appCompleted)
      throws IOException {
    long count = 0;
    long bytesParsed;
    long bytesParsedLastBatch = 0;
    boolean putError = false;
    try {
      MappingIterator<TimelineDomain> iter = objMapper.readValues(parser,
          TimelineDomain.class);

      while (iter.hasNext()) {
        TimelineDomain domain = iter.next();
        domain.setOwner(ugi.getShortUserName());
        LOG.trace("Read domain {}", domain.getId());
        ++count;
        bytesParsed = parser.getCurrentLocation().getCharOffset() + 1;
        LOG.trace("Parser now at offset {}", bytesParsed);

        try {
          tdm.putDomain(domain, ugi);
          setOffset(getOffset() + bytesParsed - bytesParsedLastBatch);
          bytesParsedLastBatch = bytesParsed;
        } catch (YarnException e) {
          putError = true;
          throw new IOException("Error posting domain", e);
        } catch (IOException e) {
          putError = true;
          throw new IOException("Error posting domain", e);
        }
      }
    } catch (IOException e) {
      // if app hasn't completed then there may be errors due to the
      // incomplete file which are treated as EOF until app completes
      if (appCompleted || putError) {
        throw e;
      }
    } catch (RuntimeException e) {
      if (appCompleted || !(e.getCause() instanceof JsonParseException)) {
        throw e;
      }
    }
    return count;
  }
}
