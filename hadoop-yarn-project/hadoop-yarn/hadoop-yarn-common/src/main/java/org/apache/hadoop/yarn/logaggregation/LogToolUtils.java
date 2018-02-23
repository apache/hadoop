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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

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


  /**
   * Create the container log file under given (local directory/nodeId) and
   * return the PrintStream object.
   * @param localDir the Local Dir
   * @param nodeId the NodeId
   * @param containerId the ContainerId
   * @return the printStream object
   * @throws IOException if an I/O error occurs
   */
  public static PrintStream createPrintStream(String localDir, String nodeId,
      String containerId) throws IOException {
    PrintStream out = System.out;
    if(localDir != null && !localDir.isEmpty()) {
      Path nodePath = new Path(localDir, LogAggregationUtils
          .getNodeString(nodeId));
      Files.createDirectories(Paths.get(nodePath.toString()));
      Path containerLogPath = new Path(nodePath, containerId);
      out = new PrintStream(containerLogPath.toString(), "UTF-8");
    }
    return out;
  }
}
