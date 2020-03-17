/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Iterator;
import java.util.Map;

@XmlRootElement(name = "clusterScalingMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterScalingMetrics {

  protected ResourceRequestCountInfo resourceRequests;

  public ClusterScalingMetrics(){
    // JAXB needs this
  }

  public ClusterScalingMetrics(final ResourceManager rm) {
    this(rm, rm.getResourceScheduler());
  }

  public ClusterScalingMetrics(final ResourceManager rm,
      final ResourceScheduler rs) {
    if (rs == null) {
      throw new NotFoundException("Null ResourceScheduler instance");
    }

    if (!(rs instanceof CapacityScheduler)) {
      throw new BadRequestException("Only Capacity Scheduler is supported!");
    }

    QueueMetrics metrics = rs.getRootQueueMetrics();
    Map<Resource, Integer> pendingContainers = metrics.getContainerAskToCount();

    resourceRequests = new ResourceRequestCountInfo();

    Iterator<Map.Entry<Resource, Integer>> it =
        pendingContainers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Resource, Integer> pendingContainer = it.next();
      resourceRequests.add(new CustomResourceInfo(pendingContainer.getKey()),
          pendingContainer.getValue());
    }
  }

  public ResourceRequestCountInfo getResourceRequests() {
    return resourceRequests;
  }
}
