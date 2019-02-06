/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.common.api.builder;

import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.submarine.common.api.JobComponentStatus;

public class JobComponentStatusBuilder {
  public static JobComponentStatus fromServiceComponent(Component component) {
    long totalAskedContainers = component.getNumberOfContainers();
    int numReadyContainers = 0;
    int numRunningButUnreadyContainers = 0;
    String compName = component.getName();

    for (Container c : component.getContainers()) {
      if (c.getState() == ContainerState.READY) {
        numReadyContainers++;
      } else if (c.getState() == ContainerState.RUNNING_BUT_UNREADY) {
        numRunningButUnreadyContainers++;
      }
    }

    return new JobComponentStatus(compName, numReadyContainers,
        numRunningButUnreadyContainers, totalAskedContainers);
  }

}
