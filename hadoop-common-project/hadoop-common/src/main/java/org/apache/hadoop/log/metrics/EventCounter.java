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
package org.apache.hadoop.log.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A log4J Appender that simply counts logging events in three levels:
 * fatal, error and warn. The class name is used in log4j.properties
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EventCounter extends AppenderSkeleton {

  /**
   * @deprecated Use EventCount.FATAL.get();
   */
  @InterfaceAudience.Private
  public static long getFatal() {
    return EventCount.FATAL.get();
  }

  /**
   * @deprecated Use EventCount.ERROR.get();
   */
  @InterfaceAudience.Private
  public static long getError() {
    return EventCount.ERROR.get();
  }

  /**
   * @deprecated Use EventCount.WARN.get();
   */
  @InterfaceAudience.Private
  public static long getWarn() {
    return EventCount.WARN.get();
  }

  /**
   * @deprecated Use EventCount.INFO.get();
   */
  @InterfaceAudience.Private
  public static long getInfo() {
    return EventCount.INFO.get();
  }

  @Override
  public void append(LoggingEvent event) {
    Level level = event.getLevel();
    // depends on the api, == might not work
    // see HADOOP-7055 for details
    if (level.equals(Level.INFO)) {
      EventCount.INFO.incr();
    }
    else if (level.equals(Level.WARN)) {
      EventCount.WARN.incr();
    }
    else if (level.equals(Level.ERROR)) {
      EventCount.ERROR.incr();
    }
    else if (level.equals(Level.FATAL)) {
      EventCount.FATAL.incr();
    }
  }

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
