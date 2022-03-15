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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;

public class WeightToWeightConverter
    implements CapacityConverter {
  private static final String ROOT_QUEUE = "root";

  @Override
  public void convertWeightsForChildQueues(FSQueue queue,
      Configuration csConfig) {
    List<FSQueue> children = queue.getChildQueues();

    if (queue instanceof FSParentQueue || !children.isEmpty()) {
      if (queue.getName().equals(ROOT_QUEUE)) {
        csConfig.set(getProperty(queue), getWeightString(queue));
      }

      children.forEach(fsQueue -> csConfig.set(
          getProperty(fsQueue), getWeightString(fsQueue)));
      csConfig.setBoolean(getAutoCreateV2EnabledProperty(queue), true);
    }
  }

  private String getProperty(FSQueue queue) {
    return PREFIX + queue.getName() + ".capacity";
  }

  private String getAutoCreateV2EnabledProperty(FSQueue queue) {
    return PREFIX + queue.getName() + ".auto-queue-creation-v2.enabled";
  }

  private String getWeightString(FSQueue queue) {
    return Float.toString(queue.getWeight()) + "w";
  }
}
