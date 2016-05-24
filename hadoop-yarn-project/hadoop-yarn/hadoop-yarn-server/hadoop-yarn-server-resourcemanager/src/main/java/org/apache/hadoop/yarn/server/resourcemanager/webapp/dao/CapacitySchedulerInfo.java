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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.nodelabels.NodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;

import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "capacityScheduler")
@XmlType(name = "capacityScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerInfo extends SchedulerInfo {

  protected float capacity;
  protected float usedCapacity;
  protected float maxCapacity;
  protected String queueName;
  protected CapacitySchedulerQueueInfoList queues;

  @XmlTransient
  static final float EPSILON = 1e-8f;

  public CapacitySchedulerInfo() {
  } // JAXB needs this

  /*
   * @param parent parent queue
   * @param nodeLabel node partition
   */
  public CapacitySchedulerInfo(final CSQueue parent, final NodeLabel nodeLabel) {
    String label = nodeLabel.getLabelName();
    QueueCapacities parentQueueCapacities = parent.getQueueCapacities();
    this.queueName = parent.getQueueName();
    this.usedCapacity = parent.getUsedCapacity(label) * 100;
    this.capacity = parentQueueCapacities.getCapacity(label) * 100;
    float max = parentQueueCapacities.getMaximumCapacity(label);
    if (max < EPSILON || max > 1f)
      max = 1f;
    this.maxCapacity = max * 100;

    queues = getQueues(parent, nodeLabel);
  }

  public float getCapacity() {
    return this.capacity;
  }

  public float getUsedCapacity() {
    return this.usedCapacity;
  }

  public float getMaxCapacity() {
    return this.maxCapacity;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public CapacitySchedulerQueueInfoList getQueues() {
    return this.queues;
  }

  /*
   * @param parent parent queue
   * @param nodeLabel node partition
   */
  protected final CapacitySchedulerQueueInfoList getQueues(final CSQueue parent,
      final NodeLabel nodeLabel) {
    CapacitySchedulerQueueInfoList queuesInfo =
        new CapacitySchedulerQueueInfoList();

    // JAXB marashalling leads to situation where the "type" field injected
    // for JSON changes from string to array depending on order of printing
    // Issue gets fixed if all the leaf queues are marshalled before the
    // non-leaf queues. See YARN-4785 for more details.
    List<CSQueue> childQueues = new ArrayList<>();
    List<CSQueue> childLeafQueues = new ArrayList<>();
    List<CSQueue> childNonLeafQueues = new ArrayList<>();
    for (CSQueue queue : parent.getChildQueues()) {
      if (!((AbstractCSQueue) queue).accessibleToPartition(nodeLabel
          .getLabelName())) {
        // Skip displaying the hierarchy for the queues for which the
        // labels are not accessible
        continue;
      }
      if (queue instanceof LeafQueue) {
        childLeafQueues.add(queue);
      } else {
        childNonLeafQueues.add(queue);
      }
    }
    childQueues.addAll(childLeafQueues);
    childQueues.addAll(childNonLeafQueues);

    for (CSQueue queue : childQueues) {
      CapacitySchedulerQueueInfo info;
      if (queue instanceof LeafQueue) {
        info =
            new CapacitySchedulerLeafQueueInfo((LeafQueue) queue,
                nodeLabel.getLabelName());
      } else {
        info = new CapacitySchedulerQueueInfo(queue, nodeLabel.getLabelName());
        info.queues = getQueues(queue, nodeLabel);
      }
      queuesInfo.addToQueueInfoList(info);
    }
    return queuesInfo;
  }

}
