/*
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

package org.apache.slider.server.appmaster.state;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.Records;

import java.util.Locale;

/**
 * Class containing the logic to build/split container priorities into the
 * different fields used by Slider
 *
 * The original design here had a requestID merged with the role, to
 * track outstanding requests. However, this isn't possible, so
 * the request ID has been dropped. A "location specified" flag was
 * added to indicate whether or not the request was for a specific location
 * -though this is currently unused.
 * 
 * The methods are effectively surplus -but retained to preserve the
 * option of changing behavior in future
 */
public final class ContainerPriority {

  // bit that represents whether location is specified
  static final int NOLOCATION = 1 << 30;
  
  public static int buildPriority(int role,
                                  boolean locationSpecified) {
    int location = locationSpecified ? 0 : NOLOCATION;
    return role | location;
  }


  public static Priority createPriority(int role,
                                        boolean locationSpecified) {
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(ContainerPriority.buildPriority(role,
                                                    locationSpecified));
    return pri;
  }

  public static int extractRole(int priority) {
    return priority >= NOLOCATION ? priority ^ NOLOCATION : priority;
  }

  /**
   * Does the priority have location
   * @param priority priority index
   * @return true if the priority has the location marker
   */
  public static boolean hasLocation(int priority) {
    return (priority ^ NOLOCATION ) == 0;
  }
  
  /**
   * Map from a container to a role key by way of its priority
   * @param container container
   * @return role key
   */
  public static int extractRole(Container container) {
    Priority priority = container.getPriority();
    return extractRole(priority);
  }
  
  /**
   * Priority record to role mapper
   * @param priorityRecord priority record
   * @return the role #
   */
  public static int extractRole(Priority priorityRecord) {
    Preconditions.checkNotNull(priorityRecord);
    return extractRole(priorityRecord.getPriority());
  }

  /**
   * Convert a priority record to a string, extracting role and locality
   * @param priorityRecord priority record. May be null
   * @return a string value
   */
  public static String toString(Priority priorityRecord) {
    if (priorityRecord==null) {
      return "(null)";
    } else {
      return String.format(Locale.ENGLISH,
          "role %d (locality=%b)",
          extractRole(priorityRecord),
          hasLocation(priorityRecord.getPriority()));
    }
  }
}
