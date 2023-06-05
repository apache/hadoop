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
package org.apache.hadoop.hdfs;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

/**
 * Used to verify that certain exceptions or messages are present in log output.
 */
public class LogVerificationAppender extends AppenderSkeleton {
  private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(final LoggingEvent loggingEvent) {
    log.add(loggingEvent);
  }

  @Override
  public void close() {
  }

  public List<LoggingEvent> getLog() {
    return new ArrayList<LoggingEvent>(log);
  }
  
  public int countExceptionsWithMessage(final String text) {
    int count = 0;
    for (LoggingEvent e: getLog()) {
      ThrowableInformation t = e.getThrowableInformation();
      if (t != null) {
        String m = t.getThrowable().getMessage();
        if (m.contains(text)) {
          count++;
        }
      }
    }
    return count;
  }

  public int countLinesWithMessage(final String text) {
    int count = 0;
    for (LoggingEvent e: getLog()) {
      String msg = e.getRenderedMessage();
      if (msg != null && msg.contains(text)) {
        count++;
      }
    }
    return count;
  }
}
