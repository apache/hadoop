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

package org.apache.hadoop.yarn.logaggregation.filecontroller.ifile;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;

import com.google.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.BoundedRangeFileInputStream;
import org.apache.hadoop.io.file.tfile.Compression;
import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationHtmlBlock;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController.IndexedFileLogMeta;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController.IndexedLogsMeta;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController.IndexedPerAggregationLogMeta;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

/**
 * The Aggregated Logs Block implementation for Indexed File.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class IndexedFileAggregatedLogsBlock extends LogAggregationHtmlBlock {

  private final LogAggregationIndexedFileController fileController;
  private final Configuration conf;

  @Inject
  public IndexedFileAggregatedLogsBlock(ViewContext ctx,
      Configuration conf,
      LogAggregationIndexedFileController fileController) {
    super(ctx);
    this.conf = conf;
    this.fileController = fileController;
  }

  @Override
  protected void render(Block html) {
    BlockParameters params = verifyAndParseParameters(html);
    if (params == null) {
      return;
    }

    ApplicationId appId = params.getAppId();
    ContainerId containerId = params.getContainerId();
    NodeId nodeId = params.getNodeId();
    String appOwner = params.getAppOwner();
    String logEntity = params.getLogEntity();
    long start = params.getStartIndex();
    long end = params.getEndIndex();
    long startTime = params.getStartTime();
    long endTime = params.getEndTime();

    List<FileStatus> nodeFiles = null;
    try {
      nodeFiles = LogAggregationUtils
          .getRemoteNodeFileList(conf, appId, appOwner,
              this.fileController.getRemoteRootLogDir(),
              this.fileController.getRemoteRootLogDirSuffix());
    } catch(Exception ex) {
      html.h1("Unable to locate any logs for container "
          + containerId.toString());
      LOG.error(ex.getMessage());
      return;
    }

    Map<String, Long> checkSumFiles;
    try {
      checkSumFiles = fileController.parseCheckSumFiles(nodeFiles);
    } catch (IOException ex) {
      LOG.error("Error getting logs for " + logEntity, ex);
      html.h1("Error getting logs for " + logEntity);
      return;
    }

    List<FileStatus> fileToRead;
    try {
      fileToRead = fileController.getNodeLogFileToRead(nodeFiles,
          nodeId.toString(), appId);
    } catch (IOException ex) {
      LOG.error("Error getting logs for " + logEntity, ex);
      html.h1("Error getting logs for " + logEntity);
      return;
    }

    boolean foundLog = false;
    String desiredLogType = $(CONTAINER_LOG_TYPE);
    try {
      for (FileStatus thisNodeFile : fileToRead) {
        Long checkSumIndex = checkSumFiles.get(
            thisNodeFile.getPath().getName());
        long endIndex = -1;
        if (checkSumIndex != null) {
          endIndex = checkSumIndex.longValue();
        }
        IndexedLogsMeta indexedLogsMeta = null;
        try {
          indexedLogsMeta = fileController.loadIndexedLogsMeta(
              thisNodeFile.getPath(), endIndex, appId);
        } catch (Exception ex) {
          // DO NOTHING
          LOG.warn("Can not load log meta from the log file:"
              + thisNodeFile.getPath());
          continue;
        }
        if (indexedLogsMeta == null) {
          continue;
        }
        Map<ApplicationAccessType, String> appAcls = indexedLogsMeta.getAcls();
        String user = indexedLogsMeta.getUser();
        String remoteUser = request().getRemoteUser();
        if (!checkAcls(conf, appId, user, appAcls, remoteUser)) {
          html.h1().__("User [" + remoteUser
              + "] is not authorized to view the logs for " + logEntity
              + " in log file [" + thisNodeFile.getPath().getName() + "]")
              .__();
          LOG.error("User [" + remoteUser
              + "] is not authorized to view the logs for " + logEntity);
          continue;
        }
        String compressAlgo = indexedLogsMeta.getCompressName();
        List<IndexedFileLogMeta> candidates = new ArrayList<>();
        for (IndexedPerAggregationLogMeta logMeta
            : indexedLogsMeta.getLogMetas()) {
          for (Entry<String, List<IndexedFileLogMeta>> meta
              : logMeta.getLogMetas().entrySet()) {
            for (IndexedFileLogMeta log : meta.getValue()) {
              if (!log.getContainerId().equals(containerId.toString())) {
                continue;
              }
              if (desiredLogType != null && !desiredLogType.isEmpty()
                  && !desiredLogType.equals(log.getFileName())) {
                continue;
              }
              candidates.add(log);
            }
          }
        }
        if (candidates.isEmpty()) {
          continue;
        }

        foundLog = readContainerLog(compressAlgo, html, thisNodeFile, start,
            end, candidates, startTime, endTime, foundLog, logEntity);
      }
      if (!foundLog) {
        if (desiredLogType.isEmpty()) {
          html.h1("No logs available for container " + containerId.toString());
        } else {
          html.h1("Unable to locate '" + desiredLogType
              + "' log for container " + containerId.toString());
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception ex) {
      html.h1().__("Error getting logs for " + logEntity).__();
      LOG.error("Error getting logs for " + logEntity, ex);
    }
  }

  private boolean readContainerLog(String compressAlgo, Block html,
      FileStatus thisNodeFile, long start, long end,
      List<IndexedFileLogMeta> candidates, long startTime, long endTime,
      boolean foundLog, String logEntity) throws IOException {
    Algorithm compressName = Compression.getCompressionAlgorithmByName(
        compressAlgo);
    Decompressor decompressor = compressName.getDecompressor();
    FileContext fileContext = FileContext.getFileContext(
        thisNodeFile.getPath().toUri(), conf);
    try (FSDataInputStream fsin = fileContext.open(thisNodeFile.getPath())) {
      int bufferSize = 65536;
      for (IndexedFileLogMeta candidate : candidates) {
        if (candidate.getLastModifiedTime() < startTime
            || candidate.getLastModifiedTime() > endTime) {
          continue;
        }
        byte[] cbuf = new byte[bufferSize];
        InputStream in = null;
        try {
          in = compressName.createDecompressionStream(
              new BoundedRangeFileInputStream(fsin, candidate.getStartIndex(),
                  candidate.getFileCompressedSize()), decompressor,
              LogAggregationIndexedFileController.getFSInputBufferSize(conf));
          long logLength = candidate.getFileSize();
          html.pre().__("\n\n").__();
          html.p().__("Log Type: " + candidate.getFileName()).__();
          html.p().__("Log Upload Time: " +
              Times.format(candidate.getLastModifiedTime())).__();
          html.p().__("Log Length: " + Long.toString(logLength)).__();

          long[] range = checkParseRange(html, start, end, startTime, endTime,
              logLength, candidate.getFileName());
          processContainerLog(html, range, in, bufferSize, cbuf);

          foundLog = true;
        } catch (Exception ex) {
          LOG.error("Error getting logs for " + logEntity, ex);
          continue;
        } finally {
          IOUtils.closeQuietly(in);
        }
      }
    }
    return foundLog;
  }

}
