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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Comparator;

import org.apache.hadoop.yarn.api.records.Priority;

/**
 * A Comparator which orders SchedulableEntities by priority.
 */
public class PriorityComparator implements Comparator<SchedulableEntity> {

  @Override
  public int compare(SchedulableEntity se1, SchedulableEntity se2) {
    Priority p1 = se1.getPriority();
    Priority p2 = se2.getPriority();
    if (p1 == null && p2 == null) {
      return 0;
    } else if (p1 == null) {
      return -1;
    } else if (p2 == null) {
      return 1;
    }
    return p1.compareTo(p2);
  }
}
