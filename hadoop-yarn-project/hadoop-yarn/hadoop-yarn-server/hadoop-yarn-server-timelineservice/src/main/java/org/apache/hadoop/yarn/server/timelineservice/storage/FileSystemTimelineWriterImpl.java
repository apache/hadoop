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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse.TimelineWriteError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * This implements a local file based backend for storing application timeline
 * information. This implementation may not provide a complete implementation of
 * all the necessary features. This implementation is provided solely for basic
 * testing purposes, and should not be used in a non-test situation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemTimelineWriterImpl extends AbstractService
    implements TimelineWriter {

  private String outputRoot;

  /** Config param for timeline service storage tmp root for FILE YARN-3264. */
  public static final String TIMELINE_SERVICE_STORAGE_DIR_ROOT
      = YarnConfiguration.TIMELINE_SERVICE_PREFIX + "fs-writer.root-dir";

  public static final String ENTITIES_DIR = "entities";

  /** Default extension for output files. */
  public static final String TIMELINE_SERVICE_STORAGE_EXTENSION = ".thist";

  /** default value for storage location on local disk. */
  private static final String STORAGE_DIR_ROOT = "timeline_service_data";

  FileSystemTimelineWriterImpl() {
    super((FileSystemTimelineWriterImpl.class.getName()));
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineEntities entities, UserGroupInformation callerUgi)
      throws IOException {
    TimelineWriteResponse response = new TimelineWriteResponse();
    String clusterId = context.getClusterId();
    String userId = context.getUserId();
    String flowName = context.getFlowName();
    String flowVersion = context.getFlowVersion();
    long flowRunId = context.getFlowRunId();
    String appId = context.getAppId();

    for (TimelineEntity entity : entities.getEntities()) {
      write(clusterId, userId, flowName, flowVersion, flowRunId, appId, entity,
          response);
    }
    return response;
  }

  private synchronized void write(String clusterId, String userId,
      String flowName, String flowVersion, long flowRun, String appId,
      TimelineEntity entity, TimelineWriteResponse response)
      throws IOException {
    PrintWriter out = null;
    try {
      String dir = mkdirs(outputRoot, ENTITIES_DIR, clusterId, userId,
          escape(flowName), escape(flowVersion), String.valueOf(flowRun), appId,
          entity.getType());
      String fileName = dir + entity.getId() +
          TIMELINE_SERVICE_STORAGE_EXTENSION;
      out =
          new PrintWriter(new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream(fileName, true), "UTF-8")));
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

  @Override
  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException {
    return null;

  }

  @VisibleForTesting
  String getOutputRoot() {
    return outputRoot;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    outputRoot = conf.get(TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        conf.get("hadoop.tmp.dir") + File.separator + STORAGE_DIR_ROOT);
  }

  @Override
  public void serviceStart() throws Exception {
    mkdirs(outputRoot, ENTITIES_DIR);
  }

  @Override
  public void flush() throws IOException {
    // no op
  }

  private static String mkdirs(String... dirStrs) throws IOException {
    StringBuilder path = new StringBuilder();
    for (String dirStr : dirStrs) {
      path.append(dirStr).append(File.separatorChar);
      File dir = new File(path.toString());
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new IOException("Could not create directories for " + dir);
        }
      }
    }
    return path.toString();
  }

  // specifically escape the separator character
  private static String escape(String str) {
    return str.replace(File.separatorChar, '_');
  }
}
