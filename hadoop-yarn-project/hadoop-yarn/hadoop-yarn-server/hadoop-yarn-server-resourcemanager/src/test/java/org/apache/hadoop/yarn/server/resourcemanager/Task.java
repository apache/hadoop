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

/**
 * 
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .TestUtils;

public class Task {
  private static final Log LOG = LogFactory.getLog(Task.class);
  
  public enum State {PENDING, ALLOCATED, RUNNING, COMPLETE};
  
  final private ApplicationId applicationId;
  final private int taskId;
  final private Priority priority;
  final private SchedulerRequestKey schedulerKey;
  
  final private Set<String> hosts = new HashSet<String>();
  final private Set<String> racks = new HashSet<String>();
  
  private ContainerId containerId;
  private org.apache.hadoop.yarn.server.resourcemanager.NodeManager nodeManager;
  
  private State state;

  public Task(Application application, Priority priority, String[] hosts) {
    this.applicationId = application.getApplicationId();
    this.priority = priority;
    
    taskId = application.getNextTaskId();
    state = State.PENDING;
    
    // Special case: Don't care about locality
    if (!(hosts.length == 1 && 
        hosts[0].equals(ResourceRequest.ANY))) {
      for (String host : hosts) {
        this.hosts.add(host);
        this.racks.add(Application.resolve(host));
      }
    }
    this.schedulerKey = TestUtils.toSchedulerKey(priority.getPriority());
    LOG.info("Task " + taskId + " added to application " + this.applicationId + 
        " with " + this.hosts.size() + " hosts, " + racks.size() + " racks");
  }
  
  public int getTaskId() {
    return taskId;
  }

  public Priority getPriority() {
    return priority;
  }

  public SchedulerRequestKey getSchedulerKey() {
    return schedulerKey;
  }
  
  public org.apache.hadoop.yarn.server.resourcemanager.NodeManager getNodeManager() {
    return nodeManager;
  }
  
  public ContainerId getContainerId() {
    return containerId;
  }
  
  public ApplicationId getApplicationID() {
    return applicationId;
  }

  public String[] getHosts() {
    return hosts.toArray(new String[hosts.size()]);
  }
  
  public String[] getRacks() {
    return racks.toArray(new String[racks.size()]);
  }
  
  public boolean canSchedule(NodeType type, String hostName) {
    if (type == NodeType.NODE_LOCAL) { 
      return hosts.contains(hostName);
    } else if (type == NodeType.RACK_LOCAL) {
      return racks.contains(Application.resolve(hostName));
    } 
    
    return true;
  }
  
  public void start(NodeManager nodeManager, ContainerId containerId) {
    this.nodeManager = nodeManager;
    this.containerId = containerId;
    setState(State.RUNNING);
  }
  
  public void stop() {
    if (getState() != State.RUNNING) {
      throw new IllegalStateException("Trying to stop a non-running task: " + 
          getTaskId() + " of application " + getApplicationID());
    }
    this.nodeManager = null;
    this.containerId = null;
    setState(State.COMPLETE);
  }
  
  public State getState() {
    return state;
  }

  private void setState(State state) {
    this.state = state;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Task) {
      return ((Task)obj).taskId == this.taskId;
    }
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return taskId;
  }
}