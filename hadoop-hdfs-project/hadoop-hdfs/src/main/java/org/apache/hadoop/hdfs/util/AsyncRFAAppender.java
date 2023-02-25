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

package org.apache.hadoop.hdfs.util;

import java.io.IOException;

import org.apache.log4j.AsyncAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Until we migrate to log4j2, use this appender for namenode audit logger as well as
 * datanode and namenode metric loggers with log4j properties, if async logging is required with
 * RFA.
 * This appender will take parameters necessary to supply RollingFileAppender to AsyncAppender.
 * While migrating to log4j2, we can directly wrap RFA appender to Async appender as part of
 * log4j2 properties. However, same is not possible with log4j1 properties.
 */
public class AsyncRFAAppender extends AsyncAppender {

  /**
   * The default maximum file size is 10MB.
   */
  private String maxFileSize = String.valueOf(10*1024*1024);

  /**
   * There is one backup file by default.
   */
  private int maxBackupIndex = 1;

  /**
   * The name of the log file.
   */
  private String fileName = null;

  private String conversionPattern = null;

  /**
   * Does appender block when buffer is full.
   */
  private boolean blocking = true;

  /**
   * Buffer size.
   */
  private int bufferSize = DEFAULT_BUFFER_SIZE;

  private RollingFileAppender rollingFileAppender = null;

  private volatile boolean isRollingFileAppenderAssigned = false;

  @Override
  public void append(LoggingEvent event) {
    if (rollingFileAppender == null) {
      appendRFAToAsyncAppender();
    }
    super.append(event);
  }

  private synchronized void appendRFAToAsyncAppender() {
    if (!isRollingFileAppenderAssigned) {
      PatternLayout patternLayout;
      if (conversionPattern != null) {
        patternLayout = new PatternLayout(conversionPattern);
      } else {
        patternLayout = new PatternLayout();
      }
      try {
        rollingFileAppender = new RollingFileAppender(patternLayout, fileName, true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      rollingFileAppender.setMaxBackupIndex(maxBackupIndex);
      rollingFileAppender.setMaxFileSize(maxFileSize);
      this.addAppender(rollingFileAppender);
      isRollingFileAppenderAssigned = true;
      super.setBlocking(blocking);
      super.setBufferSize(bufferSize);
    }
  }

  public String getMaxFileSize() {
    return maxFileSize;
  }

  public void setMaxFileSize(String maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  public int getMaxBackupIndex() {
    return maxBackupIndex;
  }

  public void setMaxBackupIndex(int maxBackupIndex) {
    this.maxBackupIndex = maxBackupIndex;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getConversionPattern() {
    return conversionPattern;
  }

  public void setConversionPattern(String conversionPattern) {
    this.conversionPattern = conversionPattern;
  }

  public boolean isBlocking() {
    return blocking;
  }

  public void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }
}
