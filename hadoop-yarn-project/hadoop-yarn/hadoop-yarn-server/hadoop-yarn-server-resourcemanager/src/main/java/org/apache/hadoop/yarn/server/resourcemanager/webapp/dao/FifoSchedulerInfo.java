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

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

@XmlRootElement(name = "fifoScheduler")
@XmlType(name = "fifoScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class FifoSchedulerInfo extends SchedulerInfo {

  protected float capacity;
  protected float usedCapacity;
  protected QueueState qstate;
  protected long minQueueMemoryCapacity;
  protected long maxQueueMemoryCapacity;
  protected int numNodes;
  protected int usedNodeCapacity;
  protected int availNodeCapacity;
  protected int totalNodeCapacity;
  protected int numContainers;

  @XmlTransient
  protected String qstateFormatted;

  @XmlTransient
  protected String qName;

  public FifoSchedulerInfo() {
  } // JAXB needs this

  public FifoSchedulerInfo(final ResourceManager rm) {

    RMContext rmContext = rm.getRMContext();

    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();
    qName = fs.getQueueInfo("", false, false).getQueueName();
    QueueInfo qInfo = fs.getQueueInfo(qName, true, true);

    this.usedCapacity = qInfo.getCurrentCapacity();
    this.capacity = qInfo.getCapacity();
    this.minQueueMemoryCapacity = fs.getMinimumResourceCapability().getMemorySize();
    this.maxQueueMemoryCapacity = fs.getMaximumResourceCapability().getMemorySize();
    this.qstate = qInfo.getQueueState();

    this.numNodes = rmContext.getRMNodes().size();
    this.usedNodeCapacity = 0;
    this.availNodeCapacity = 0;
    this.totalNodeCapacity = 0;
    this.numContainers = 0;

    for (RMNode ni : rmContext.getRMNodes().values()) {
      SchedulerNodeReport report = fs.getNodeReport(ni.getNodeID());
      this.usedNodeCapacity += report.getUsedResource().getMemorySize();
      this.availNodeCapacity += report.getAvailableResource().getMemorySize();
      this.totalNodeCapacity += ni.getTotalCapability().getMemorySize();
      this.numContainers += fs.getNodeReport(ni.getNodeID()).getNumContainers();
    }

    this.schedulerName = "Fifo Scheduler";
  }

  public int getNumNodes() {
    return this.numNodes;
  }

  public int getUsedNodeCapacity() {
    return this.usedNodeCapacity;
  }

  public int getAvailNodeCapacity() {
    return this.availNodeCapacity;
  }

  public int getTotalNodeCapacity() {
    return this.totalNodeCapacity;
  }

  public int getNumContainers() {
    return this.numContainers;
  }

  public String getState() {
    return this.qstate.toString();
  }

  public String getQueueName() {
    return this.qName;
  }

  public long getMinQueueMemoryCapacity() {
    return this.minQueueMemoryCapacity;
  }

  public long getMaxQueueMemoryCapacity() {
    return this.maxQueueMemoryCapacity;
  }

  public float getCapacity() {
    return this.capacity;
  }

  public float getUsedCapacity() {
    return this.usedCapacity;
  }

}
