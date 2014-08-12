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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Order {@link FSAppAttempt} objects by priority and then by submit time, as
 * in the default scheduler in Hadoop.
 */
@Private
@Unstable
public class FifoAppComparator implements Comparator<FSAppAttempt>, Serializable {
  private static final long serialVersionUID = 3428835083489547918L;

  public int compare(FSAppAttempt a1, FSAppAttempt a2) {
    int res = a1.getPriority().compareTo(a2.getPriority());
    if (res == 0) {
      if (a1.getStartTime() < a2.getStartTime()) {
        res = -1;
      } else {
        res = (a1.getStartTime() == a2.getStartTime() ? 0 : 1);
      }
    }
    if (res == 0) {
      // If there is a tie, break it by app ID to get a deterministic order
      res = a1.getApplicationId().compareTo(a2.getApplicationId());
    }
    return res;
  }
}
