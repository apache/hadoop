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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse.TimelineWriteError;
import org.apache.hadoop.yarn.client.api.impl.FileSystemTimelineWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implements a FileSystem based backend for storing application timeline
 * information. This implementation may not provide a complete implementation of
 * all the necessary features. This implementation is provided solely for basic
 * testing purposes, and should not be used in a non-test situation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemTimelineWriterImpl extends AbstractService
    implements TimelineWriter {

  /** Config param for timeline service storage tmp root for FILE YARN-3264. */
  public static final String TIMELINE_SERVICE_STORAGE_DIR_ROOT =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "fs-writer.root-dir";

  public static final String TIMELINE_FS_WRITER_NUM_RETRIES =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "fs-writer.num-retries";
  public static final int DEFAULT_TIMELINE_FS_WRITER_NUM_RETRIES = 0;

  public static final String TIMELINE_FS_WRITER_RETRY_INTERVAL_MS =
       YarnConfiguration.TIMELINE_SERVICE_PREFIX +
               "fs-writer.retry-interval-ms";
  public static final long DEFAULT_TIMELINE_FS_WRITER_RETRY_INTERVAL_MS = 1000L;

  public static final String ENTITIES_DIR = "entities";

  /** Default extension for output files. */
  public static final String TIMELINE_SERVICE_STORAGE_EXTENSION = ".thist";

  private FileSystem fs;
  private Path rootPath;
  private int fsNumRetries;
  private long fsRetryInterval;
  private Path entitiesPath;
  private Configuration config;

  /** default value for storage location on local disk. */
  private static final String STORAGE_DIR_ROOT = "timeline_service_data";

  private static final Logger LOG =
          LoggerFactory.getLogger(FileSystemTimelineWriter.class);

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
      writeInternal(clusterId, userId, flowName, flowVersion,
              flowRunId, appId, entity, response);
    }
    return response;
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineDomain domain)
      throws IOException {
    // TODO implementation for storing domain into FileSystem
    return null;
  }

  private synchronized void writeInternal(String clusterId, String userId,
                                          String flowName, String flowVersion,
                                          long flowRun, String appId,
                                          TimelineEntity entity,
                                          TimelineWriteResponse response)
                                          throws IOException {
    String entityTypePathStr = clusterId + File.separator + userId +
        File.separator + escape(flowName) + File.separator +
        escape(flowVersion) + File.separator + flowRun + File.separator + appId
        + File.separator + entity.getType();
    Path entityTypePath = new Path(entitiesPath, entityTypePathStr);
    try {
      mkdirs(entityTypePath);
      Path filePath =
              new Path(entityTypePath,
                      entity.getId() + TIMELINE_SERVICE_STORAGE_EXTENSION);
      createFileWithRetries(filePath);

      byte[] record =  new StringBuilder()
              .append(TimelineUtils.dumpTimelineRecordtoJSON(entity))
              .append("\n").toString().getBytes("UTF-8");
      writeFileWithRetries(filePath, record);
    } catch (Exception ioe) {
      LOG.warn("Interrupted operation:" + ioe.getMessage());
      TimelineWriteError error = createTimelineWriteError(entity);
      /*
       * TODO: set an appropriate error code after PoC could possibly be:
       * error.setErrorCode(TimelineWriteError.IO_EXCEPTION);
       */
      response.addError(error);
    }
  }

  private TimelineWriteError createTimelineWriteError(TimelineEntity entity) {
    TimelineWriteError error = new TimelineWriteError();
    error.setEntityId(entity.getId());
    error.setEntityType(entity.getType());
    return error;
  }

  @Override
  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException {
    return null;
  }

  @VisibleForTesting
  String getOutputRoot() {
    return rootPath.toString();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    String outputRoot = conf.get(TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        conf.get("hadoop.tmp.dir") + File.separator + STORAGE_DIR_ROOT);
    rootPath = new Path(outputRoot);
    entitiesPath = new Path(rootPath, ENTITIES_DIR);
    fsNumRetries = conf.getInt(TIMELINE_FS_WRITER_NUM_RETRIES,
            DEFAULT_TIMELINE_FS_WRITER_NUM_RETRIES);
    fsRetryInterval = conf.getLong(TIMELINE_FS_WRITER_RETRY_INTERVAL_MS,
            DEFAULT_TIMELINE_FS_WRITER_RETRY_INTERVAL_MS);
    config = conf;
    fs = rootPath.getFileSystem(config);
  }

  @Override
  public void serviceStart() throws Exception {
    mkdirsWithRetries(rootPath);
    mkdirsWithRetries(entitiesPath);
  }

  @Override
  public void flush() throws IOException {
    // no op
  }

  private void mkdirs(Path... paths) throws IOException, InterruptedException {
    for (Path path: paths) {
      if (!existsWithRetries(path)) {
        mkdirsWithRetries(path);
      }
    }
  }

  // Code from FSRMStateStore.
  private void mkdirsWithRetries(final Path dirPath)
          throws IOException, InterruptedException {
    new FSAction<Void>() {
      @Override
      public Void run() throws IOException {
        fs.mkdirs(dirPath);
        return null;
      }
    }.runWithRetries();
  }

  private void writeFileWithRetries(final Path outputPath, final byte[] data)
          throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws IOException {
        writeFile(outputPath, data);
        return null;
      }
    }.runWithRetries();
  }

  private boolean createFileWithRetries(final Path newFile)
          throws IOException, InterruptedException {
    return new FSAction<Boolean>() {
      @Override
      public Boolean run() throws IOException {
        return createFile(newFile);
      }
    }.runWithRetries();
  }

  private boolean existsWithRetries(final Path path)
          throws IOException, InterruptedException {
    return new FSAction<Boolean>() {
      @Override
      public Boolean run() throws IOException {
        return fs.exists(path);
      }
    }.runWithRetries();
  }

  private abstract class FSAction<T> {
    abstract T run() throws IOException;

    T runWithRetries() throws IOException, InterruptedException {
      int retry = 0;
      while (true) {
        try {
          return run();
        } catch (IOException e) {
          LOG.info("Exception while executing a FS operation.", e);
          if (++retry > fsNumRetries) {
            LOG.info("Maxed out FS retries. Giving up!");
            throw e;
          }
          LOG.info("Will retry operation on FS. Retry no. " + retry +
              " after sleeping for " + fsRetryInterval + " seconds");
          Thread.sleep(fsRetryInterval);
        }
      }
    }
  }

  private boolean createFile(Path newFile) throws IOException {
    return fs.createNewFile(newFile);
  }

  /**
   * In order to make this writeInternal atomic as a part of writeInternal
   * we will first writeInternal data to .tmp file and then rename it.
   * Here we are assuming that rename is atomic for underlying file system.
   */
  protected void writeFile(Path outputPath, byte[] data) throws IOException {
    Path tempPath =
            new Path(outputPath.getParent(), outputPath.getName() + ".tmp");
    FSDataOutputStream fsOut = null;
    // This file will be overwritten when app/attempt finishes for saving the
    // final status.
    try {
      fsOut = fs.create(tempPath, true);
      FSDataInputStream fsIn = fs.open(outputPath);
      IOUtils.copyBytes(fsIn, fsOut, config, false);
      fsIn.close();
      fs.delete(outputPath, false);
      fsOut.write(data);
      fsOut.close();
      fs.rename(tempPath, outputPath);
    } catch (IOException ie) {
      LOG.error("Got an exception while writing file", ie);
    }
  }

  // specifically escape the separator character
  private static String escape(String str) {
    return str.replace(File.separatorChar, '_');
  }
}
