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
package org.apache.hadoop.http;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.AppenderSkeleton;

/**
 * Log4j Appender adapter for HttpRequestLog
 */
public class HttpRequestLogAppender extends AppenderSkeleton {

  private String filename;
  private int retainDays;

  public HttpRequestLogAppender() {
  }

  public void setRetainDays(int retainDays) {
    this.retainDays = retainDays;
  }

  public int getRetainDays() {
    return retainDays;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  @Override
  public void append(LoggingEvent event) {
  }

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
