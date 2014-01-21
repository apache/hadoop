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

package org.apache.hadoop.yarn;

import java.io.File;
import java.io.Flushable;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.log4j.FileAppender;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A simple log4j-appender for container's logs.
 * 
 */
@Public
@Unstable
public class ContainerLogAppender extends FileAppender
  implements Flushable
{
  private String containerLogDir;
  //so that log4j can configure it from the configuration(log4j.properties). 
  private int maxEvents;
  private Queue<LoggingEvent> tail = null;

  @Override
  public void activateOptions() {
    synchronized (this) {
      if (maxEvents > 0) {
        tail = new LinkedList<LoggingEvent>();
      }
      setFile(new File(this.containerLogDir, "syslog").toString());
      setAppend(true);
      super.activateOptions();
    }
  }
  
  @Override
  public void append(LoggingEvent event) {
    synchronized (this) {
      if (tail == null) {
        super.append(event);
      } else {
        if (tail.size() >= maxEvents) {
          tail.remove();
        }
        tail.add(event);
      }
    }
  }
  
  @Override
  public void flush() {
    if (qw != null) {
      qw.flush();
    }
  }

  @Override
  public synchronized void close() {
    if (tail != null) {
      for(LoggingEvent event: tail) {
        super.append(event);
      }
    }
    super.close();
  }

  /**
   * Getter/Setter methods for log4j.
   */
  
  public String getContainerLogDir() {
    return this.containerLogDir;
  }

  public void setContainerLogDir(String containerLogDir) {
    this.containerLogDir = containerLogDir;
  }

  private static final int EVENT_SIZE = 100;
  
  public long getTotalLogFileSize() {
    return maxEvents * EVENT_SIZE;
  }

  public void setTotalLogFileSize(long logSize) {
    maxEvents = (int) logSize / EVENT_SIZE;
  }
}
