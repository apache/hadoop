/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.
    OZONE_DB_CHECKPOINT_REQUEST_FLUSH;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the current checkpoint Snapshot of the OM DB. (tar.gz)
 */
public class OMDBCheckpointServlet extends HttpServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;

  private transient DBStore omDbStore;
  private transient OMMetrics omMetrics;
  private transient DataTransferThrottler throttler = null;

  @Override
  public void init() throws ServletException {

    OzoneManager om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);

    if (om == null) {
      LOG.error("Unable to initialize OMDBCheckpointServlet. OM is null");
      return;
    }

    omDbStore = om.getMetadataManager().getStore();
    omMetrics = om.getMetrics();

    OzoneConfiguration configuration = om.getConfiguration();
    long transferBandwidth = configuration.getLongBytes(
        OMConfigKeys.OZONE_DB_CHECKPOINT_TRANSFER_RATE_KEY,
        OMConfigKeys.OZONE_DB_CHECKPOINT_TRANSFER_RATE_DEFAULT);

    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
  }

  /**
   * Process a GET request for the Ozone Manager DB checkpoint snapshot.
   *
   * @param request  The servlet request we are processing
   * @param response The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {

    LOG.info("Received request to obtain OM DB checkpoint snapshot");
    if (omDbStore == null) {
      LOG.error(
          "Unable to process metadata snapshot request. DB Store is null");
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      return;
    }

    FileInputStream checkpointFileInputStream = null;
    File checkPointTarFile = null;
    try {

      boolean flush = false;
      String flushParam =
          request.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH);
      if (StringUtils.isNotEmpty(flushParam)) {
        flush = Boolean.valueOf(flushParam);
      }

      DBCheckpoint checkpoint = omDbStore.getCheckpoint(flush);
      if (checkpoint == null) {
        LOG.error("Unable to process metadata snapshot request. " +
            "Checkpoint request returned null.");
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        return;
      }
      omMetrics.setLastCheckpointCreationTimeTaken(
          checkpoint.checkpointCreationTimeTaken());

      Instant start = Instant.now();
      checkPointTarFile = OmUtils.createTarFile(
          checkpoint.getCheckpointLocation());
      Instant end = Instant.now();

      long duration = Duration.between(start, end).toMillis();
      LOG.debug("Time taken to archive the checkpoint : " + duration +
          " milliseconds");
      LOG.info("Checkpoint Tar location = " +
          checkPointTarFile.getAbsolutePath());
      omMetrics.setLastCheckpointTarOperationTimeTaken(duration);

      response.setContentType("application/x-tgz");
      response.setHeader("Content-Disposition",
          "attachment; filename=\"" +
              checkPointTarFile.getName() + "\"");

      checkpointFileInputStream = new FileInputStream(checkPointTarFile);
      start = Instant.now();
      TransferFsImage.copyFileToStream(response.getOutputStream(),
          checkPointTarFile,
          checkpointFileInputStream,
          throttler);
      end = Instant.now();

      duration = Duration.between(start, end).toMillis();
      LOG.debug("Time taken to write the checkpoint to response output " +
          "stream: " + duration + " milliseconds");
      omMetrics.setLastCheckpointStreamingTimeTaken(duration);

      checkpoint.cleanupCheckpoint();
    } catch (IOException e) {
      LOG.error(
          "Unable to process metadata snapshot request. ", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } finally {
      if (checkPointTarFile != null) {
        FileUtils.deleteQuietly(checkPointTarFile);
      }
      IOUtils.closeStream(checkpointFileInputStream);
    }
  }

}
