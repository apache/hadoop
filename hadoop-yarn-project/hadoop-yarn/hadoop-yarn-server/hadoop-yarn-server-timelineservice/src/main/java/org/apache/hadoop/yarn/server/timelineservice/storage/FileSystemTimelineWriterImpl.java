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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse.TimelineWriteError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * This implements a local file based backend for storing application timeline
 * information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemTimelineWriterImpl extends AbstractService
    implements TimelineWriter {

  private String outputRoot;

  /** Config param for timeline service storage tmp root for FILE YARN-3264 */
  public static final String TIMELINE_SERVICE_STORAGE_DIR_ROOT
    = YarnConfiguration.TIMELINE_SERVICE_PREFIX + "fs-writer.root-dir";

  /** default value for storage location on local disk */
  public static final String DEFAULT_TIMELINE_SERVICE_STORAGE_DIR_ROOT
    = "/tmp/timeline_service_data/";

  /** Default extension for output files */
  public static final String TIMELINE_SERVICE_STORAGE_EXTENSION = ".thist";

  FileSystemTimelineWriterImpl() {
    super((FileSystemTimelineWriterImpl.class.getName()));
  }

  /**
   * Stores the entire information in {@link TimelineEntity} to the
   * timeline store. Any errors occurring for individual write request objects
   * will be reported in the response.
   * 
   * @param data
   *          a {@link TimelineEntity} object
   * @return {@link TimelineWriteResponse} object.
   * @throws IOException
   */
  @Override
  public TimelineWriteResponse write(TimelineEntities entities)
      throws IOException {
    TimelineWriteResponse response = new TimelineWriteResponse();
    for (TimelineEntity entity : entities.getEntities()) {
      write(entity, response);
    }
    return response;
  }

  private void write(TimelineEntity entity,
      TimelineWriteResponse response) throws IOException {
    PrintWriter out = null;
    try {
      File outputDir = new File(outputRoot + entity.getType());
      String fileName = outputDir + "/" + entity.getId()
          + TIMELINE_SERVICE_STORAGE_EXTENSION;
      if (!outputDir.exists()) {
        if (!outputDir.mkdirs()) {
          throw new IOException("Could not create directories for " + fileName);
        }
      }
      out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
      out.println(TimelineUtils.dumpTimelineRecordtoJSON(entity));
      out.write("\n");
    } catch (IOException ioe) {
      TimelineWriteError error = new TimelineWriteError();
      error.setEntityId(entity.getId());
      error.setEntityType(entity.getType());
      /*
       * TODO: set an appropriate error code after PoC could possibly be:
       * error.setErrorCode(TimelineWriteError.IO_EXCEPTION);
       */
      response.addError(error);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Aggregates the entity information to the timeline store based on which
   * track this entity is to be rolled up to The tracks along which aggregations
   * are to be done are given by {@link TimelineAggregationTrack}
   * 
   * Any errors occurring for individual write request objects will be reported
   * in the response.
   * 
   * @param data
   *          a {@link TimelineEntity} object
   *          a {@link TimelineAggregationTrack} enum value
   * @return a {@link TimelineWriteResponse} object.
   * @throws IOException
   */
  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException {
    return null;

  }

  public String getOutputRoot() {
    return outputRoot;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    outputRoot = conf.get(TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        DEFAULT_TIMELINE_SERVICE_STORAGE_DIR_ROOT);
  }
}
