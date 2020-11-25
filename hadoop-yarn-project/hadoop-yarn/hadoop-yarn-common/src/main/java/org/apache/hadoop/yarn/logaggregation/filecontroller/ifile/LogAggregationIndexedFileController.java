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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFs;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.BoundedRangeFileInputStream;
import org.apache.hadoop.io.file.tfile.Compression;
import org.apache.hadoop.io.file.tfile.SimpleBufferedOutputStream;
import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.LogToolUtils;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.View.ViewContext;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Indexed Log Aggregation File Format implementation.
 *
 */
@Private
@Unstable
public class LogAggregationIndexedFileController
    extends LogAggregationFileController {

  private static final Logger LOG = LoggerFactory.getLogger(
      LogAggregationIndexedFileController.class);
  private static final String FS_OUTPUT_BUF_SIZE_ATTR =
      "indexedFile.fs.output.buffer.size";
  private static final String FS_INPUT_BUF_SIZE_ATTR =
      "indexedFile.fs.input.buffer.size";
  private static final String FS_NUM_RETRIES_ATTR =
      "indexedFile.fs.op.num-retries";
  private static final String FS_RETRY_INTERVAL_MS_ATTR =
      "indexedFile.fs.retry-interval-ms";
  private static final String LOG_ROLL_OVER_MAX_FILE_SIZE_GB =
      "indexedFile.log.roll-over.max-file-size-gb";
  private static final int LOG_ROLL_OVER_MAX_FILE_SIZE_GB_DEFAULT = 10;

  @VisibleForTesting
  public static final String CHECK_SUM_FILE_SUFFIX = "-checksum";

  private int fsNumRetries = 3;
  private long fsRetryInterval = 1000L;
  private static final int VERSION = 1;
  private IndexedLogsMeta indexedLogsMeta = null;
  private IndexedPerAggregationLogMeta logsMetaInThisCycle;
  private long logAggregationTimeInThisCycle;
  private FSDataOutputStream fsDataOStream;
  private Algorithm compressAlgo;
  private CachedIndexedLogsMeta cachedIndexedLogsMeta = null;
  private boolean logAggregationSuccessfullyInThisCyCle = false;
  private long currentOffSet = 0;
  private Path remoteLogCheckSumFile;
  private FileContext fc;
  private UserGroupInformation ugi;
  private byte[] uuid = null;
  private final int UUID_LENGTH = 32;
  private long logRollOverMaxFileSize;
  private Clock sysClock;

  public LogAggregationIndexedFileController() {}

  @Override
  public void initInternal(Configuration conf) {
    String compressName = conf.get(
        YarnConfiguration.NM_LOG_AGG_COMPRESSION_TYPE,
        YarnConfiguration.DEFAULT_NM_LOG_AGG_COMPRESSION_TYPE);
    this.compressAlgo = Compression.getCompressionAlgorithmByName(
        compressName);
    this.fsNumRetries = conf.getInt(FS_NUM_RETRIES_ATTR, 3);
    this.fsRetryInterval = conf.getLong(FS_RETRY_INTERVAL_MS_ATTR, 1000L);
    this.logRollOverMaxFileSize = getRollOverLogMaxSize(conf);
    this.sysClock = getSystemClock();
  }

  @Override
  public void initializeWriter(
      final LogAggregationFileControllerContext context)
      throws IOException {
    final UserGroupInformation userUgi = context.getUserUgi();
    final Map<ApplicationAccessType, String> appAcls = context.getAppAcls();
    final String nodeId = context.getNodeId().toString();
    final ApplicationId appId = context.getAppId();
    final Path remoteLogFile = context.getRemoteNodeLogFileForApp();
    this.ugi = userUgi;
    logAggregationSuccessfullyInThisCyCle = false;
    logsMetaInThisCycle = new IndexedPerAggregationLogMeta();
    logAggregationTimeInThisCycle = this.sysClock.getTime();
    logsMetaInThisCycle.setUploadTimeStamp(logAggregationTimeInThisCycle);
    logsMetaInThisCycle.setRemoteNodeFile(remoteLogFile.getName());
    try {
      userUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          fc = FileContext.getFileContext(
              remoteRootLogDir.toUri(), conf);
          fc.setUMask(APP_LOG_FILE_UMASK);
          if (indexedLogsMeta == null) {
            indexedLogsMeta = new IndexedLogsMeta();
            indexedLogsMeta.setVersion(VERSION);
            indexedLogsMeta.setUser(userUgi.getShortUserName());
            indexedLogsMeta.setAcls(appAcls);
            indexedLogsMeta.setNodeId(nodeId);
            String compressName = conf.get(
                YarnConfiguration.NM_LOG_AGG_COMPRESSION_TYPE,
                YarnConfiguration.DEFAULT_NM_LOG_AGG_COMPRESSION_TYPE);
            indexedLogsMeta.setCompressName(compressName);
          }
          Path aggregatedLogFile = null;
          Pair<Path, Boolean> initializationResult = null;
          boolean createdNew;

          if (context.isLogAggregationInRolling()) {
            // In rolling log aggregation we need special initialization
            // done in initializeWriterInRolling.
            initializationResult = initializeWriterInRolling(
                remoteLogFile, appId, nodeId);
            aggregatedLogFile = initializationResult.getLeft();
            createdNew = initializationResult.getRight();
          } else {
            aggregatedLogFile = remoteLogFile;
            fsDataOStream = fc.create(remoteLogFile,
                EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                new Options.CreateOpts[] {});
            if (uuid == null) {
              uuid = createUUID(appId);
            }
            fsDataOStream.write(uuid);
            fsDataOStream.flush();
            createdNew = true;
          }

          // If we have created a new file, we know that the offset is zero.
          // Otherwise we should get this information through getFileStatus.
          if (createdNew) {
            currentOffSet = 0;
          } else {
            long aggregatedLogFileLength = fc.getFileStatus(
                aggregatedLogFile).getLen();
            // append a simple character("\n") to move the writer cursor, so
            // we could get the correct position when we call
            // fsOutputStream.getStartPos()
            final byte[] dummyBytes = "\n".getBytes(Charset.forName("UTF-8"));
            fsDataOStream.write(dummyBytes);
            fsDataOStream.flush();

            if (fsDataOStream.getPos() < (aggregatedLogFileLength
                + dummyBytes.length)) {
              currentOffSet = fc.getFileStatus(
                      aggregatedLogFile).getLen();
            } else {
              currentOffSet = 0;
            }
          }
          return null;
        }
      });
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Initializes the write for the log aggregation controller in the
   * rolling case. It sets up / modifies checksum and meta files if needed.
   *
   * @param remoteLogFile the Path of the remote log file
   * @param appId the application id
   * @param nodeId the node id
   * @return a Pair of Path and Boolean - the Path is path of the
   *         aggregated log file, while the Boolean is whether a new
   *         file was created or not
   * @throws Exception
   */
  private Pair<Path, Boolean> initializeWriterInRolling(
      final Path remoteLogFile, final ApplicationId appId,
      final String nodeId) throws Exception {
    boolean createdNew = false;
    Path aggregatedLogFile = null;
    // check uuid
    // if we can not find uuid, we would load the uuid
    // from previous aggregated log files, and at the same
    // time, we would delete any aggregated log files which
    // has invalid uuid.
    if (uuid == null) {
      uuid = loadUUIDFromLogFile(fc, remoteLogFile.getParent(),
            appId, nodeId);
    }
    Path currentRemoteLogFile = getCurrentRemoteLogFile(
        fc, remoteLogFile.getParent(), nodeId);
    // check checksum file
    boolean overwriteCheckSum = true;
    remoteLogCheckSumFile = new Path(remoteLogFile.getParent(),
        (remoteLogFile.getName() + CHECK_SUM_FILE_SUFFIX));
    if(fc.util().exists(remoteLogCheckSumFile)) {
      // if the checksum file exists, we should reset cached
      // indexedLogsMeta.
      indexedLogsMeta.getLogMetas().clear();
      if (currentRemoteLogFile != null) {
        FSDataInputStream checksumFileInputStream = null;
        try {
          checksumFileInputStream = fc.open(remoteLogCheckSumFile);
          int nameLength = checksumFileInputStream.readInt();
          byte[] b = new byte[nameLength];
          int actualLength = checksumFileInputStream.read(b);
          if (actualLength == nameLength) {
            String recoveredLogFile = new String(
                b, Charset.forName("UTF-8"));
            if (recoveredLogFile.equals(
                currentRemoteLogFile.getName())) {
              overwriteCheckSum = false;
              long endIndex = checksumFileInputStream.readLong();
              IndexedLogsMeta recoveredLogsMeta = loadIndexedLogsMeta(
                  currentRemoteLogFile, endIndex, appId);
              if (recoveredLogsMeta != null) {
                indexedLogsMeta = recoveredLogsMeta;
              }
            }
          }
        } finally {
          IOUtils.cleanupWithLogger(LOG, checksumFileInputStream);
        }
      }
    }
    // check whether we need roll over old logs
    if (currentRemoteLogFile == null || isRollover(
        fc, currentRemoteLogFile)) {
      indexedLogsMeta.getLogMetas().clear();
      overwriteCheckSum = true;
      aggregatedLogFile = new Path(remoteLogFile.getParent(),
          remoteLogFile.getName() + "_" + sysClock.getTime());
      fsDataOStream = fc.create(aggregatedLogFile,
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
          new Options.CreateOpts[] {});
      // writes the uuid
      fsDataOStream.write(uuid);
      fsDataOStream.flush();
      createdNew = true;
    } else {
      aggregatedLogFile = currentRemoteLogFile;
      fsDataOStream = fc.create(currentRemoteLogFile,
          EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND),
          new Options.CreateOpts[] {});
    }
    // recreate checksum file if needed before aggregate the logs
    if (overwriteCheckSum) {
      long currentAggregatedLogFileLength;
      if (createdNew) {
        currentAggregatedLogFileLength = 0;
      } else {
        currentAggregatedLogFileLength = fc
            .getFileStatus(aggregatedLogFile).getLen();
      }
      FSDataOutputStream checksumFileOutputStream = null;
      try {
        checksumFileOutputStream = fc.create(remoteLogCheckSumFile,
            EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
            new Options.CreateOpts[] {});
        String fileName = aggregatedLogFile.getName();
        checksumFileOutputStream.writeInt(fileName.length());
        checksumFileOutputStream.write(fileName.getBytes(
            Charset.forName("UTF-8")));
        checksumFileOutputStream.writeLong(
            currentAggregatedLogFileLength);
        checksumFileOutputStream.flush();
      } finally {
        IOUtils.cleanupWithLogger(LOG, checksumFileOutputStream);
      }
    }

    return Pair.of(aggregatedLogFile, createdNew);
  }

  @Override
  public void closeWriter() {
    IOUtils.cleanupWithLogger(LOG, this.fsDataOStream);
  }

  @Override
  public void write(LogKey logKey, LogValue logValue) throws IOException {
    String containerId = logKey.toString();
    Set<File> pendingUploadFiles = logValue
        .getPendingLogFilesToUploadForThisContainer();
    List<IndexedFileLogMeta> metas = new ArrayList<>();
    for (File logFile : pendingUploadFiles) {
      FileInputStream in = null;
      try {
        in = SecureIOUtils.openForRead(logFile, logValue.getUser(), null);
      } catch (IOException e) {
        logErrorMessage(logFile, e);
        IOUtils.cleanupWithLogger(LOG, in);
        continue;
      }
      final long fileLength = logFile.length();
      IndexedFileOutputStreamState outputStreamState = null;
      try {
        outputStreamState = new IndexedFileOutputStreamState(
            this.compressAlgo, this.fsDataOStream, conf, this.currentOffSet);
        byte[] buf = new byte[65535];
        int len = 0;
        long bytesLeft = fileLength;
        while ((len = in.read(buf)) != -1) {
          //If buffer contents within fileLength, write
          if (len < bytesLeft) {
            outputStreamState.getOutputStream().write(buf, 0, len);
            bytesLeft-=len;
          } else {
            //else only write contents within fileLength, then exit early
            outputStreamState.getOutputStream().write(buf, 0,
                (int)bytesLeft);
            break;
          }
        }
        long newLength = logFile.length();
        if(fileLength < newLength) {
          LOG.warn("Aggregated logs truncated by approximately "+
              (newLength-fileLength) +" bytes.");
        }
        logAggregationSuccessfullyInThisCyCle = true;
      } catch (IOException e) {
        String message = logErrorMessage(logFile, e);
        if (outputStreamState != null &&
            outputStreamState.getOutputStream() != null) {
          outputStreamState.getOutputStream().write(
              message.getBytes(Charset.forName("UTF-8")));
        }
      } finally {
        IOUtils.cleanupWithLogger(LOG, in);
      }

      IndexedFileLogMeta meta = new IndexedFileLogMeta();
      meta.setContainerId(containerId.toString());
      meta.setFileName(logFile.getName());
      if (outputStreamState != null) {
        outputStreamState.finish();
        meta.setFileCompressedSize(outputStreamState.getCompressedSize());
        meta.setStartIndex(outputStreamState.getStartPos());
        meta.setFileSize(fileLength);
      }
      meta.setLastModifiedTime(logFile.lastModified());
      metas.add(meta);
    }
    logsMetaInThisCycle.addContainerLogMeta(containerId, metas);
  }

  @Override
  public void postWrite(LogAggregationFileControllerContext record)
      throws Exception {
    // always aggregate the previous logsMeta, and append them together
    // at the end of the file
    indexedLogsMeta.addLogMeta(logsMetaInThisCycle);
    byte[] b = SerializationUtils.serialize(indexedLogsMeta);
    this.fsDataOStream.write(b);
    int length = b.length;
    this.fsDataOStream.writeInt(length);
    this.fsDataOStream.write(uuid);
    if (logAggregationSuccessfullyInThisCyCle &&
        record.isLogAggregationInRolling()) {
      deleteFileWithRetries(fc, ugi, remoteLogCheckSumFile);
    }
  }

  private void deleteFileWithRetries(final FileContext fileContext,
      final UserGroupInformation userUgi,
      final Path deletePath) throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        deleteFileWithPrivilege(fileContext, userUgi, deletePath);
        return null;
      }
    }.runWithRetries();
  }

  private void deleteFileWithRetries(final FileContext fileContext,
      final Path deletePath) throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        if (fileContext.util().exists(deletePath)) {
          fileContext.delete(deletePath, false);
        }
        return null;
      }
    }.runWithRetries();
  }

  private void truncateFileWithRetries(final FileContext fileContext,
      final Path truncatePath, final long newLength) throws Exception {
    new FSAction<Void>() {
      @Override
      public Void run() throws Exception {
        fileContext.truncate(truncatePath, newLength);
        return null;
      }
    }.runWithRetries();
  }

  private Object deleteFileWithPrivilege(final FileContext fileContext,
      final UserGroupInformation userUgi, final Path fileToDelete)
      throws Exception {
    return userUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        if (fileContext.util().exists(fileToDelete)) {
          fileContext.delete(fileToDelete, false);
        }
        return null;
      }
    });
  }

  @Override
  public boolean readAggregatedLogs(ContainerLogsRequest logRequest,
      OutputStream os) throws IOException {
    boolean findLogs = false;
    boolean createPrintStream = (os == null);
    ApplicationId appId = logRequest.getAppId();
    String nodeId = logRequest.getNodeId();
    String nodeIdStr = (nodeId == null || nodeId.isEmpty()) ? null
        : LogAggregationUtils.getNodeString(nodeId);
    List<String> logTypes = new ArrayList<>();
    if (logRequest.getLogTypes() != null && !logRequest
        .getLogTypes().isEmpty()) {
      logTypes.addAll(logRequest.getLogTypes());
    }
    String containerIdStr = logRequest.getContainerId();
    boolean getAllContainers = (containerIdStr == null
        || containerIdStr.isEmpty());
    long size = logRequest.getBytes();
    RemoteIterator<FileStatus> nodeFiles = LogAggregationUtils
        .getRemoteNodeFileDir(conf, appId, logRequest.getAppOwner(),
        this.remoteRootLogDir, this.remoteRootLogDirSuffix);
    if (!nodeFiles.hasNext()) {
      throw new IOException("There is no available log file for "
          + "application:" + appId);
    }
    List<FileStatus> allFiles = getAllNodeFiles(nodeFiles, appId);
    if (allFiles.isEmpty()) {
      throw new IOException("There is no available log file for "
          + "application:" + appId);
    }
    Map<String, Long> checkSumFiles = parseCheckSumFiles(allFiles);
    List<FileStatus> fileToRead = getNodeLogFileToRead(
        allFiles, nodeIdStr, appId);
    byte[] buf = new byte[65535];
    for (FileStatus thisNodeFile : fileToRead) {
      String nodeName = thisNodeFile.getPath().getName();
      Long checkSumIndex = checkSumFiles.get(nodeName);
      long endIndex = -1;
      if (checkSumIndex != null) {
        endIndex = checkSumIndex.longValue();
      }
      IndexedLogsMeta indexedLogsMeta = null;
      try {
        indexedLogsMeta = loadIndexedLogsMeta(thisNodeFile.getPath(),
            endIndex, appId);
      } catch (Exception ex) {
        // DO NOTHING
        LOG.warn("Can not load log meta from the log file:"
            + thisNodeFile.getPath() + "\n" + ex.getMessage());
        continue;
      }
      if (indexedLogsMeta == null) {
        continue;
      }
      String compressAlgo = indexedLogsMeta.getCompressName();
      List<IndexedFileLogMeta> candidates = new ArrayList<>();
      for (IndexedPerAggregationLogMeta logMeta
          : indexedLogsMeta.getLogMetas()) {
        for (Entry<String, List<IndexedFileLogMeta>> meta
            : logMeta.getLogMetas().entrySet()) {
          for (IndexedFileLogMeta log : meta.getValue()) {
            if (!getAllContainers && !log.getContainerId()
                .equals(containerIdStr)) {
              continue;
            }
            if (logTypes != null && !logTypes.isEmpty() &&
                !logTypes.contains(log.getFileName())) {
              continue;
            }
            candidates.add(log);
          }
        }
      }
      if (candidates.isEmpty()) {
        continue;
      }

      Algorithm compressName = Compression.getCompressionAlgorithmByName(
          compressAlgo);
      Decompressor decompressor = compressName.getDecompressor();
      FileContext fileContext = FileContext.getFileContext(
          thisNodeFile.getPath().toUri(), conf);
      FSDataInputStream fsin = fileContext.open(thisNodeFile.getPath());
      String currentContainer = "";
      for (IndexedFileLogMeta candidate : candidates) {
        if (!candidate.getContainerId().equals(currentContainer)) {
          if (createPrintStream) {
            closePrintStream(os);
            os = LogToolUtils.createPrintStream(
                logRequest.getOutputLocalDir(),
                thisNodeFile.getPath().getName(),
                candidate.getContainerId());
            currentContainer = candidate.getContainerId();
          }
        }
        InputStream in = null;
        try {
          in = compressName.createDecompressionStream(
              new BoundedRangeFileInputStream(fsin,
                  candidate.getStartIndex(),
                  candidate.getFileCompressedSize()),
              decompressor, getFSInputBufferSize(conf));
          LogToolUtils.outputContainerLog(candidate.getContainerId(),
              nodeName, candidate.getFileName(), candidate.getFileSize(), size,
              Times.format(candidate.getLastModifiedTime()),
              in, os, buf, ContainerLogAggregationType.AGGREGATED);
          byte[] b = aggregatedLogSuffix(candidate.getFileName())
              .getBytes(Charset.forName("UTF-8"));
          os.write(b, 0, b.length);
          findLogs = true;
        } catch (IOException e) {
          System.err.println(e.getMessage());
          compressName.returnDecompressor(decompressor);
          continue;
        } finally {
          os.flush();
          IOUtils.cleanupWithLogger(LOG, in);
        }
      }
    }
    return findLogs;
  }

  @Override
  public List<ContainerLogMeta> readAggregatedLogsMeta(
      ContainerLogsRequest logRequest) throws IOException {
    List<IndexedLogsMeta> listOfLogsMeta = new ArrayList<>();
    List<ContainerLogMeta> containersLogMeta = new ArrayList<>();
    String containerIdStr = logRequest.getContainerId();
    String nodeId = logRequest.getNodeId();
    ApplicationId appId = logRequest.getAppId();
    String appOwner = logRequest.getAppOwner();
    ApplicationAttemptId appAttemptId = logRequest.getAppAttemptId();
    boolean getAllContainers = (containerIdStr == null ||
        containerIdStr.isEmpty());
    String nodeIdStr = (nodeId == null || nodeId.isEmpty()) ? null
        : LogAggregationUtils.getNodeString(nodeId);
    RemoteIterator<FileStatus> nodeFiles = LogAggregationUtils
        .getRemoteNodeFileDir(conf, appId, appOwner, this.remoteRootLogDir,
        this.remoteRootLogDirSuffix);
    if (!nodeFiles.hasNext()) {
      throw new IOException("There is no available log file for "
          + "application:" + appId);
    }
    List<FileStatus> allFiles = getAllNodeFiles(nodeFiles, appId);
    if (allFiles.isEmpty()) {
      throw new IOException("There is no available log file for "
          + "application:" + appId);
    }
    Map<String, Long> checkSumFiles = parseCheckSumFiles(allFiles);
    List<FileStatus> fileToRead = getNodeLogFileToRead(
        allFiles, nodeIdStr, appId);
    for(FileStatus thisNodeFile : fileToRead) {
      try {
        Long checkSumIndex = checkSumFiles.get(
            thisNodeFile.getPath().getName());
        long endIndex = -1;
        if (checkSumIndex != null) {
          endIndex = checkSumIndex.longValue();
        }
        IndexedLogsMeta current = loadIndexedLogsMeta(
            thisNodeFile.getPath(), endIndex, appId);
        if (current != null) {
          listOfLogsMeta.add(current);
        }
      } catch (IOException ex) {
        // DO NOTHING
        LOG.warn("Can not get log meta from the log file:"
            + thisNodeFile.getPath() + "\n" + ex.getMessage());
      }
    }
    for (IndexedLogsMeta indexedLogMeta : listOfLogsMeta) {
      String curNodeId = indexedLogMeta.getNodeId();
      for (IndexedPerAggregationLogMeta logMeta :
          indexedLogMeta.getLogMetas()) {
        if (getAllContainers) {
          for (Entry<String, List<IndexedFileLogMeta>> log : logMeta
              .getLogMetas().entrySet()) {
            String currentContainerIdStr = log.getKey();
            if (appAttemptId != null &&
                !belongsToAppAttempt(appAttemptId, currentContainerIdStr)) {
              continue;
            }
            ContainerLogMeta meta = new ContainerLogMeta(
                log.getKey(), curNodeId);
            for (IndexedFileLogMeta aMeta : log.getValue()) {
              meta.addLogMeta(aMeta.getFileName(), Long.toString(
                  aMeta.getFileSize()),
                  Times.format(aMeta.getLastModifiedTime()));
            }
            containersLogMeta.add(meta);
          }
        } else if (logMeta.getContainerLogMeta(containerIdStr) != null) {
          ContainerLogMeta meta = new ContainerLogMeta(containerIdStr,
              curNodeId);
          for (IndexedFileLogMeta log :
              logMeta.getContainerLogMeta(containerIdStr)) {
            meta.addLogMeta(log.getFileName(), Long.toString(
                log.getFileSize()),
                Times.format(log.getLastModifiedTime()));
          }
          containersLogMeta.add(meta);
        }
      }
    }
    Collections.sort(containersLogMeta, new Comparator<ContainerLogMeta>() {
      @Override
      public int compare(ContainerLogMeta o1, ContainerLogMeta o2) {
        return o1.getContainerId().compareTo(o2.getContainerId());
      }
    });
    return containersLogMeta;
  }

  @Private
  public Map<String, Long> parseCheckSumFiles(
      List<FileStatus> fileList) throws IOException {
    Map<String, Long> checkSumFiles = new HashMap<>();
    Set<FileStatus> status =
        new HashSet<>(fileList).stream().filter(
            next -> next.getPath().getName().endsWith(
                CHECK_SUM_FILE_SUFFIX)).collect(
            Collectors.toSet());

    FileContext fc = null;
    for (FileStatus file : status) {
      FSDataInputStream checksumFileInputStream = null;
      try {
        if (fc == null) {
          fc = FileContext.getFileContext(file.getPath().toUri(), conf);
        }
        String nodeName = null;
        long index = 0L;
        checksumFileInputStream = fc.open(file.getPath());
        int nameLength = checksumFileInputStream.readInt();
        byte[] b = new byte[nameLength];
        int actualLength = checksumFileInputStream.read(b);
        if (actualLength == nameLength) {
          nodeName = new String(b, Charset.forName("UTF-8"));
          index = checksumFileInputStream.readLong();
        } else {
          continue;
        }
        if (nodeName != null && !nodeName.isEmpty()) {
          checkSumFiles.put(nodeName, Long.valueOf(index));
        }
      } catch (IOException ex) {
        LOG.warn(ex.getMessage());
        continue;
      } finally {
        IOUtils.cleanupWithLogger(LOG, checksumFileInputStream);
      }
    }
    return checkSumFiles;
  }

  @Private
  public List<FileStatus> getNodeLogFileToRead(
      List<FileStatus> nodeFiles, String nodeId, ApplicationId appId)
      throws IOException {
    List<FileStatus> listOfFiles = new ArrayList<>();
    for (FileStatus thisNodeFile : nodeFiles) {
      String nodeName = thisNodeFile.getPath().getName();
      if ((nodeId == null || nodeId.isEmpty()
          || nodeName.contains(LogAggregationUtils
          .getNodeString(nodeId))) && !nodeName.endsWith(
              LogAggregationUtils.TMP_FILE_SUFFIX) &&
          !nodeName.endsWith(CHECK_SUM_FILE_SUFFIX)) {
        listOfFiles.add(thisNodeFile);
      }
    }
    return listOfFiles;
  }

  private List<FileStatus> getAllNodeFiles(
      RemoteIterator<FileStatus> nodeFiles, ApplicationId appId)
      throws IOException {
    List<FileStatus> listOfFiles = new ArrayList<>();
    while (nodeFiles != null && nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      String nodeName = thisNodeFile.getPath().getName();
      if (nodeName.equals(appId + ".har")) {
        Path p = new Path("har:///"
            + thisNodeFile.getPath().toUri().getRawPath());
        nodeFiles = HarFs.get(p.toUri(), conf).listStatusIterator(p);
        continue;
      }
      listOfFiles.add(thisNodeFile);
    }
    return listOfFiles;
  }

  @Private
  public FileStatus getAllChecksumFiles(Map<String, FileStatus> fileMap,
      String fileName) {
    for (Entry<String, FileStatus> file : fileMap.entrySet()) {
      if (file.getKey().startsWith(fileName) && file.getKey()
          .endsWith(CHECK_SUM_FILE_SUFFIX)) {
        return file.getValue();
      }
    }
    return null;
  }

  @Override
  public void renderAggregatedLogsBlock(Block html, ViewContext context) {
    IndexedFileAggregatedLogsBlock block = new IndexedFileAggregatedLogsBlock(
        context, this.conf, this);
    block.render(html);
  }

  @Override
  public String getApplicationOwner(Path aggregatedLogPath,
      ApplicationId appId)
      throws IOException {
    if (this.cachedIndexedLogsMeta == null
        || !this.cachedIndexedLogsMeta.getRemoteLogPath()
            .equals(aggregatedLogPath)) {
      this.cachedIndexedLogsMeta = new CachedIndexedLogsMeta(
          loadIndexedLogsMeta(aggregatedLogPath, appId), aggregatedLogPath);
    }
    return this.cachedIndexedLogsMeta.getCachedIndexedLogsMeta().getUser();
  }

  @Override
  public Map<ApplicationAccessType, String> getApplicationAcls(
      Path aggregatedLogPath, ApplicationId appId) throws IOException {
    if (this.cachedIndexedLogsMeta == null
        || !this.cachedIndexedLogsMeta.getRemoteLogPath()
            .equals(aggregatedLogPath)) {
      this.cachedIndexedLogsMeta = new CachedIndexedLogsMeta(
          loadIndexedLogsMeta(aggregatedLogPath, appId), aggregatedLogPath);
    }
    return this.cachedIndexedLogsMeta.getCachedIndexedLogsMeta().getAcls();
  }

  @Override
  public Path getRemoteAppLogDir(ApplicationId appId, String user)
      throws IOException {
    return LogAggregationUtils.getRemoteAppLogDir(conf, appId, user,
        this.remoteRootLogDir, this.remoteRootLogDirSuffix);
  }

  @Override
  public Path getOlderRemoteAppLogDir(ApplicationId appId, String user)
      throws IOException {
    return LogAggregationUtils.getOlderRemoteAppLogDir(conf, appId, user,
        this.remoteRootLogDir, this.remoteRootLogDirSuffix);
  }

  @Private
  public IndexedLogsMeta loadIndexedLogsMeta(Path remoteLogPath, long end,
      ApplicationId appId) throws IOException {
    FileContext fileContext =
        FileContext.getFileContext(remoteLogPath.toUri(), conf);
    FSDataInputStream fsDataIStream = null;
    try {
      fsDataIStream = fileContext.open(remoteLogPath);
      if (end == 0) {
        return null;
      }
      long fileLength = end < 0 ? fileContext.getFileStatus(
          remoteLogPath).getLen() : end;

      fsDataIStream.seek(fileLength - Integer.SIZE/ Byte.SIZE - UUID_LENGTH);
      int offset = fsDataIStream.readInt();
      // If the offset/log meta size is larger than 64M,
      // output a warn message for better debug.
      if (offset > 64 * 1024 * 1024) {
        LOG.warn("The log meta size read from " + remoteLogPath
            + " is " + offset);
      }

      // Load UUID and make sure the UUID is correct.
      byte[] uuidRead = new byte[UUID_LENGTH];
      int uuidReadLen = fsDataIStream.read(uuidRead);
      if (this.uuid == null) {
        this.uuid = createUUID(appId);
      }
      if (uuidReadLen != UUID_LENGTH || !Arrays.equals(this.uuid, uuidRead)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("the length of loaded UUID:{}", uuidReadLen);
          LOG.debug("the loaded UUID:{}", new String(uuidRead,
              Charset.forName("UTF-8")));
          LOG.debug("the expected UUID:{}", new String(this.uuid,
              Charset.forName("UTF-8")));
        }
        throw new IOException("The UUID from "
            + remoteLogPath + " is not correct. The offset of loaded UUID is "
            + (fileLength - UUID_LENGTH));
      }

      // Load Log Meta
      byte[] array = new byte[offset];
      fsDataIStream.seek(
          fileLength - offset - Integer.SIZE/ Byte.SIZE - UUID_LENGTH);
      fsDataIStream.readFully(array);
      int actual = array.length;
      if (actual != offset) {
        throw new IOException("Error on loading log meta from "
            + remoteLogPath);
      }
      return (IndexedLogsMeta)SerializationUtils
          .deserialize(array);
    } finally {
      IOUtils.cleanupWithLogger(LOG, fsDataIStream);
    }
  }

  private IndexedLogsMeta loadIndexedLogsMeta(Path remoteLogPath,
      ApplicationId appId) throws IOException {
    return loadIndexedLogsMeta(remoteLogPath, -1, appId);
  }

  /**
   * This IndexedLogsMeta includes all the meta information
   * for the aggregated log file.
   */
  @Private
  @VisibleForTesting
  public static class IndexedLogsMeta implements Serializable {

    private static final long serialVersionUID = 5439875373L;
    private int version;
    private String user;
    private String compressName;
    private Map<ApplicationAccessType, String> acls;
    private String nodeId;
    private List<IndexedPerAggregationLogMeta> logMetas = new ArrayList<>();

    public int getVersion() {
      return this.version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    public String getUser() {
      return this.user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public Map<ApplicationAccessType, String> getAcls() {
      return this.acls;
    }

    public void setAcls(Map<ApplicationAccessType, String> acls) {
      this.acls = acls;
    }

    public String getCompressName() {
      return compressName;
    }

    public void setCompressName(String compressName) {
      this.compressName = compressName;
    }

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }

    public void addLogMeta(IndexedPerAggregationLogMeta logMeta) {
      logMetas.add(logMeta);
    }

    public List<IndexedPerAggregationLogMeta> getLogMetas() {
      return logMetas;
    }
  }

  /**
   * This IndexedPerAggregationLogMeta includes the meta information
   * for all files which would be aggregated in one
   * Log aggregation cycle.
   */
  public static class IndexedPerAggregationLogMeta implements Serializable {
    private static final long serialVersionUID = 3929298383L;
    private String remoteNodeLogFileName;
    private Map<String, List<IndexedFileLogMeta>> logMetas = new HashMap<>();
    private long uploadTimeStamp;

    public String getRemoteNodeFile() {
      return remoteNodeLogFileName;
    }
    public void setRemoteNodeFile(String remoteNodeLogFileName) {
      this.remoteNodeLogFileName = remoteNodeLogFileName;
    }

    public void addContainerLogMeta(String containerId,
        List<IndexedFileLogMeta> logMeta) {
      logMetas.put(containerId, logMeta);
    }

    public List<IndexedFileLogMeta> getContainerLogMeta(String containerId) {
      return logMetas.get(containerId);
    }

    public Map<String, List<IndexedFileLogMeta>> getLogMetas() {
      return logMetas;
    }

    public long getUploadTimeStamp() {
      return uploadTimeStamp;
    }

    public void setUploadTimeStamp(long uploadTimeStamp) {
      this.uploadTimeStamp = uploadTimeStamp;
    }
  }

  /**
   * This IndexedFileLogMeta includes the meta information
   * for a single file which would be aggregated in one
   * Log aggregation cycle.
   *
   */
  @Private
  @VisibleForTesting
  public static class IndexedFileLogMeta implements Serializable {
    private static final long serialVersionUID = 1L;
    private String containerId;
    private String fileName;
    private long fileSize;
    private long fileCompressedSize;
    private long lastModifiedTime;
    private long startIndex;

    public String getFileName() {
      return fileName;
    }
    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public long getFileSize() {
      return fileSize;
    }
    public void setFileSize(long fileSize) {
      this.fileSize = fileSize;
    }

    public long getFileCompressedSize() {
      return fileCompressedSize;
    }
    public void setFileCompressedSize(long fileCompressedSize) {
      this.fileCompressedSize = fileCompressedSize;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }
    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }

    public long getStartIndex() {
      return startIndex;
    }
    public void setStartIndex(long startIndex) {
      this.startIndex = startIndex;
    }

    public String getContainerId() {
      return containerId;
    }
    public void setContainerId(String containerId) {
      this.containerId = containerId;
    }
  }

  private static String logErrorMessage(File logFile, Exception e) {
    String message = "Error aggregating log file. Log file : "
        + logFile.getAbsolutePath() + ". " + e.getMessage();
    LOG.error(message, e);
    return message;
  }

  private static class IndexedFileOutputStreamState {
    private final Algorithm compressAlgo;
    private Compressor compressor;
    private final FSDataOutputStream fsOut;
    private long posStart;
    private final SimpleBufferedOutputStream fsBufferedOutput;
    private OutputStream out;
    private long offset;

    IndexedFileOutputStreamState(Algorithm compressionName,
        FSDataOutputStream fsOut, Configuration conf, long offset)
        throws IOException {
      this.compressAlgo = compressionName;
      this.fsOut = fsOut;
      this.offset = offset;
      this.posStart = fsOut.getPos();

      BytesWritable fsOutputBuffer = new BytesWritable();
      fsOutputBuffer.setCapacity(LogAggregationIndexedFileController
          .getFSOutputBufferSize(conf));

      this.fsBufferedOutput = new SimpleBufferedOutputStream(this.fsOut,
          fsOutputBuffer.getBytes());

      this.compressor = compressAlgo.getCompressor();

      try {
        this.out = compressAlgo.createCompressionStream(
            fsBufferedOutput, compressor, 0);
      } catch (IOException e) {
        LOG.warn(e.getMessage());
        compressAlgo.returnCompressor(compressor);
        throw e;
      }
    }

    OutputStream getOutputStream() {
      return out;
    }

    long getCurrentPos() throws IOException {
      return fsOut.getPos() + fsBufferedOutput.size();
    }

    long getStartPos() {
      return posStart + offset;
    }

    long getCompressedSize() throws IOException {
      long ret = getCurrentPos() - posStart;
      return ret;
    }

    void finish() throws IOException {
      try {
        if (out != null) {
          out.flush();
          out = null;
        }
      } finally {
        compressAlgo.returnCompressor(compressor);
        compressor = null;
      }
    }
  }

  private static class CachedIndexedLogsMeta {
    private final Path remoteLogPath;
    private final IndexedLogsMeta indexedLogsMeta;
    CachedIndexedLogsMeta(IndexedLogsMeta indexedLogsMeta,
        Path remoteLogPath) {
      this.indexedLogsMeta = indexedLogsMeta;
      this.remoteLogPath = remoteLogPath;
    }

    public Path getRemoteLogPath() {
      return this.remoteLogPath;
    }

    public IndexedLogsMeta getCachedIndexedLogsMeta() {
      return this.indexedLogsMeta;
    }
  }

  @Private
  public static int getFSOutputBufferSize(Configuration conf) {
    return conf.getInt(FS_OUTPUT_BUF_SIZE_ATTR, 256 * 1024);
  }

  @Private
  public static int getFSInputBufferSize(Configuration conf) {
    return conf.getInt(FS_INPUT_BUF_SIZE_ATTR, 256 * 1024);
  }

  @Private
  @VisibleForTesting
  public long getRollOverLogMaxSize(Configuration conf) {
    boolean supportAppend = false;
    try {
      FileSystem fs = FileSystem.get(remoteRootLogDir.toUri(), conf);
      if (fs instanceof LocalFileSystem || fs.hasPathCapability(
          remoteRootLogDir, CommonPathCapabilities.FS_APPEND)) {
        supportAppend = true;
      }
    } catch (Exception ioe) {
      LOG.warn("Unable to determine if the filesystem supports " +
          "append operation", ioe);
    }
    if (supportAppend) {
      return 1024L * 1024 * 1024 * conf.getInt(
          LOG_ROLL_OVER_MAX_FILE_SIZE_GB,
          LOG_ROLL_OVER_MAX_FILE_SIZE_GB_DEFAULT);
    } else {
      return 0L;
    }
  }

  private abstract class FSAction<T> {
    abstract T run() throws Exception;

    T runWithRetries() throws Exception {
      int retry = 0;
      while (true) {
        try {
          return run();
        } catch (IOException e) {
          LOG.info("Exception while executing an FS operation.", e);
          if (++retry > fsNumRetries) {
            LOG.info("Maxed out FS retries. Giving up!");
            throw e;
          }
          LOG.info("Retrying operation on FS. Retry no. " + retry);
          Thread.sleep(fsRetryInterval);
        }
      }
    }
  }

  private Path getCurrentRemoteLogFile(final FileContext fc,
      final Path parent, final String nodeId) throws IOException {
    RemoteIterator<FileStatus> files = fc.listStatus(parent);
    long maxTime = 0L;
    Path returnPath = null;
    while(files.hasNext()) {
      FileStatus candidate = files.next();
      String fileName = candidate.getPath().getName();
      if (fileName.contains(LogAggregationUtils.getNodeString(nodeId))
          && !fileName.endsWith(LogAggregationUtils.TMP_FILE_SUFFIX) &&
          !fileName.endsWith(CHECK_SUM_FILE_SUFFIX)) {
        if (candidate.getModificationTime() > maxTime) {
          maxTime = candidate.getModificationTime();
          returnPath = candidate.getPath();
        }
      }
    }
    return returnPath;
  }

  private byte[] loadUUIDFromLogFile(final FileContext fc,
      final Path parent, final ApplicationId appId, final String nodeId)
      throws Exception {
    byte[] id = null;
    RemoteIterator<FileStatus> files = fc.listStatus(parent);
    FSDataInputStream fsDataInputStream = null;
    byte[] uuid = createUUID(appId);
    while(files.hasNext()) {
      try {
        Path checkPath = files.next().getPath();
        if (checkPath.getName().contains(LogAggregationUtils
            .getNodeString(nodeId)) && !checkPath.getName()
                .endsWith(CHECK_SUM_FILE_SUFFIX)) {
          fsDataInputStream = fc.open(checkPath);
          byte[] b = new byte[uuid.length];
          int actual = fsDataInputStream.read(b);
          if (actual != uuid.length || Arrays.equals(b, uuid)) {
            deleteFileWithRetries(fc, checkPath);
          } else if (id == null){
            id = uuid;
          }
        }
      } finally {
        IOUtils.cleanupWithLogger(LOG, fsDataInputStream);
      }
    }
    return id == null ? uuid : id;
  }

  @Private
  @VisibleForTesting
  public boolean isRollover(final FileContext fc,
      final Path candidate) throws IOException {
    FileStatus fs = fc.getFileStatus(candidate);
    return fs.getLen() >= this.logRollOverMaxFileSize;
  }

  @Private
  @VisibleForTesting
  public Clock getSystemClock() {
    return SystemClock.getInstance();
  }

  private byte[] createUUID(ApplicationId appId) throws IOException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return digest.digest(appId.toString().getBytes(
          Charset.forName("UTF-8")));
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException(ex);
    }
  }
}
