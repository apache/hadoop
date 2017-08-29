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
package org.apache.hadoop.yarn.logaggregation;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.util.Times;

/**
 * This class contains several utility function which could be used in different
 * log tools.
 *
 */
public final class LogToolUtils {

  private LogToolUtils() {}

  public static final String CONTAINER_ON_NODE_PATTERN =
      "Container: %s on %s";

  /**
   * Return a list of {@link ContainerLogMeta} for a container
   * from Remote FileSystem.
   *
   * @param conf the configuration
   * @param appId the applicationId
   * @param containerIdStr the containerId
   * @param nodeId the nodeId
   * @param appOwner the application owner
   * @return a list of {@link ContainerLogMeta}
   * @throws IOException if there is no available log file
   */
  public static List<ContainerLogMeta> getContainerLogMetaFromRemoteFS(
      Configuration conf, ApplicationId appId, String containerIdStr,
      String nodeId, String appOwner) throws IOException {
    List<ContainerLogMeta> containersLogMeta = new ArrayList<>();
    boolean getAllContainers = (containerIdStr == null);
    String nodeIdStr = (nodeId == null) ? null
        : LogAggregationUtils.getNodeString(nodeId);
    RemoteIterator<FileStatus> nodeFiles = LogAggregationUtils
        .getRemoteNodeFileDir(conf, appId, appOwner);
    if (nodeFiles == null) {
      throw new IOException("There is no available log fils for "
          + "application:" + appId);
    }
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      if (nodeIdStr != null) {
        if (!thisNodeFile.getPath().getName().contains(nodeIdStr)) {
          continue;
        }
      }
      if (!thisNodeFile.getPath().getName()
          .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
        AggregatedLogFormat.LogReader reader =
            new AggregatedLogFormat.LogReader(conf,
            thisNodeFile.getPath());
        try {
          DataInputStream valueStream;
          LogKey key = new LogKey();
          valueStream = reader.next(key);
          while (valueStream != null) {
            if (getAllContainers || (key.toString().equals(containerIdStr))) {
              ContainerLogMeta containerLogMeta = new ContainerLogMeta(
                  key.toString(), thisNodeFile.getPath().getName());
              while (true) {
                try {
                  Pair<String, String> logMeta =
                      LogReader.readContainerMetaDataAndSkipData(
                          valueStream);
                  containerLogMeta.addLogMeta(
                      logMeta.getFirst(),
                      logMeta.getSecond(),
                      Times.format(thisNodeFile.getModificationTime()));
                } catch (EOFException eof) {
                  break;
                }
              }
              containersLogMeta.add(containerLogMeta);
              if (!getAllContainers) {
                break;
              }
            }
            // Next container
            key = new LogKey();
            valueStream = reader.next(key);
          }
        } finally {
          reader.close();
        }
      }
    }
    return containersLogMeta;
  }

  /**
   * Output container log.
   * @param containerId the containerId
   * @param nodeId the nodeId
   * @param fileName the log file name
   * @param fileLength the log file length
   * @param outputSize the output size
   * @param lastModifiedTime the log file last modified time
   * @param fis the log file input stream
   * @param os the output stream
   * @param buf the buffer
   * @param logType the log type.
   * @throws IOException if we can not access the log file.
   */
  public static void outputContainerLog(String containerId, String nodeId,
      String fileName, long fileLength, long outputSize,
      String lastModifiedTime, InputStream fis, OutputStream os,
      byte[] buf, ContainerLogAggregationType logType) throws IOException {
    long toSkip = 0;
    long totalBytesToRead = fileLength;
    long skipAfterRead = 0;
    if (outputSize < 0) {
      long absBytes = Math.abs(outputSize);
      if (absBytes < fileLength) {
        toSkip = fileLength - absBytes;
        totalBytesToRead = absBytes;
      }
      org.apache.hadoop.io.IOUtils.skipFully(fis, toSkip);
    } else {
      if (outputSize < fileLength) {
        totalBytesToRead = outputSize;
        skipAfterRead = fileLength - outputSize;
      }
    }

    long curRead = 0;
    long pendingRead = totalBytesToRead - curRead;
    int toRead = pendingRead > buf.length ? buf.length
        : (int) pendingRead;
    int len = fis.read(buf, 0, toRead);
    boolean keepGoing = (len != -1 && curRead < totalBytesToRead);
    if (keepGoing) {
      StringBuilder sb = new StringBuilder();
      String containerStr = String.format(
          LogToolUtils.CONTAINER_ON_NODE_PATTERN,
          containerId, nodeId);
      sb.append(containerStr + "\n");
      sb.append("LogAggregationType: " + logType + "\n");
      sb.append(StringUtils.repeat("=", containerStr.length()) + "\n");
      sb.append("LogType:" + fileName + "\n");
      sb.append("LogLastModifiedTime:" + lastModifiedTime + "\n");
      sb.append("LogLength:" + Long.toString(fileLength) + "\n");
      sb.append("LogContents:\n");
      byte[] b = sb.toString().getBytes(
          Charset.forName("UTF-8"));
      os.write(b, 0, b.length);
    }
    while (keepGoing) {
      os.write(buf, 0, len);
      curRead += len;

      pendingRead = totalBytesToRead - curRead;
      toRead = pendingRead > buf.length ? buf.length
          : (int) pendingRead;
      len = fis.read(buf, 0, toRead);
      keepGoing = (len != -1 && curRead < totalBytesToRead);
    }
    org.apache.hadoop.io.IOUtils.skipFully(fis, skipAfterRead);
    os.flush();
  }

  public static void outputContainerLogThroughZeroCopy(String containerId,
      String nodeId, String fileName, long fileLength, long outputSize,
      String lastModifiedTime, FileInputStream fis, OutputStream os,
      ContainerLogAggregationType logType) throws IOException {
    long toSkip = 0;
    long totalBytesToRead = fileLength;
    if (outputSize < 0) {
      long absBytes = Math.abs(outputSize);
      if (absBytes < fileLength) {
        toSkip = fileLength - absBytes;
        totalBytesToRead = absBytes;
      }
    } else {
      if (outputSize < fileLength) {
        totalBytesToRead = outputSize;
      }
    }

    if (totalBytesToRead > 0) {
      // output log summary
      StringBuilder sb = new StringBuilder();
      String containerStr = String.format(
          LogToolUtils.CONTAINER_ON_NODE_PATTERN,
          containerId, nodeId);
      sb.append(containerStr + "\n");
      sb.append("LogAggregationType: " + logType + "\n");
      sb.append(StringUtils.repeat("=", containerStr.length()) + "\n");
      sb.append("LogType:" + fileName + "\n");
      sb.append("LogLastModifiedTime:" + lastModifiedTime + "\n");
      sb.append("LogLength:" + Long.toString(fileLength) + "\n");
      sb.append("LogContents:\n");
      byte[] b = sb.toString().getBytes(
          Charset.forName("UTF-8"));
      os.write(b, 0, b.length);
      // output log content
      FileChannel inputChannel = fis.getChannel();
      WritableByteChannel outputChannel = Channels.newChannel(os);
      long position = toSkip;
      while (totalBytesToRead > 0) {
        long transferred =
            inputChannel.transferTo(position, totalBytesToRead, outputChannel);
        totalBytesToRead -= transferred;
        position += transferred;
      }
      os.flush();
    }
  }

  public static boolean outputAggregatedContainerLog(Configuration conf,
      ApplicationId appId, String appOwner,
      String containerId, String nodeId,
      String logFileName, long outputSize, OutputStream os,
      byte[] buf) throws IOException {
    boolean findLogs = false;
    RemoteIterator<FileStatus> nodeFiles = LogAggregationUtils
        .getRemoteNodeFileDir(conf, appId, appOwner);
    while (nodeFiles != null && nodeFiles.hasNext()) {
      final FileStatus thisNodeFile = nodeFiles.next();
      String nodeName = thisNodeFile.getPath().getName();
      if (nodeName.equals(appId + ".har")) {
        Path p = new Path("har:///"
            + thisNodeFile.getPath().toUri().getRawPath());
        nodeFiles = HarFs.get(p.toUri(), conf).listStatusIterator(p);
        continue;
      }
      if ((nodeId == null || nodeName.contains(LogAggregationUtils
          .getNodeString(nodeId))) && !nodeName.endsWith(
              LogAggregationUtils.TMP_FILE_SUFFIX)) {
        AggregatedLogFormat.LogReader reader = null;
        try {
          reader = new AggregatedLogFormat.LogReader(conf,
              thisNodeFile.getPath());
          DataInputStream valueStream;
          LogKey key = new LogKey();
          valueStream = reader.next(key);
          while (valueStream != null && !key.toString()
              .equals(containerId)) {
            // Next container
            key = new LogKey();
            valueStream = reader.next(key);
          }
          if (valueStream == null) {
            continue;
          }
          while (true) {
            try {
              String fileType = valueStream.readUTF();
              String fileLengthStr = valueStream.readUTF();
              long fileLength = Long.parseLong(fileLengthStr);
              if (fileType.equalsIgnoreCase(logFileName)) {
                LogToolUtils.outputContainerLog(containerId,
                    nodeId, fileType, fileLength, outputSize,
                    Times.format(thisNodeFile.getModificationTime()),
                    valueStream, os, buf,
                    ContainerLogAggregationType.AGGREGATED);
                StringBuilder sb = new StringBuilder();
                String endOfFile = "End of LogType:" + fileType;
                sb.append("\n" + endOfFile + "\n");
                sb.append(StringUtils.repeat("*", endOfFile.length() + 50)
                    + "\n\n");
                byte[] b = sb.toString().getBytes(Charset.forName("UTF-8"));
                os.write(b, 0, b.length);
                findLogs = true;
              } else {
                long totalSkipped = 0;
                long currSkipped = 0;
                while (currSkipped != -1 && totalSkipped < fileLength) {
                  currSkipped = valueStream.skip(
                      fileLength - totalSkipped);
                  totalSkipped += currSkipped;
                }
              }
            } catch (EOFException eof) {
              break;
            }
          }
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
    }
    os.flush();
    return findLogs;
  }
}
