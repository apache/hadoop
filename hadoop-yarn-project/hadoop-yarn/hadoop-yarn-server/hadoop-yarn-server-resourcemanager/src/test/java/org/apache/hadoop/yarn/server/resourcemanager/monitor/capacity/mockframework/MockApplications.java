/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager.User;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class MockApplications {
  private static final Logger LOG = LoggerFactory.getLogger(
      MockApplications.class);

  private String config;
  private ResourceCalculator resourceCalculator;
  private Map<String, CSQueue> nameToCSQueues;
  private Map<String, Resource> partitionToResource;
  private Map<NodeId, FiCaSchedulerNode> nodeIdToSchedulerNodes;
  private Map<String, Set<String>> userMap = new HashMap<>();
  private Map<String, Map<String, HashMap<String, ResourceUsage>>> userResourceUsagePerLabel = new HashMap<>();
  private int id = 1;

  MockApplications(String appsConfig,
      ResourceCalculator resourceCalculator,
      Map<String, CSQueue> nameToCSQueues,
      Map<String, Resource> partitionToResource,
      Map<NodeId, FiCaSchedulerNode> nodeIdToSchedulerNodes) {
    this.config = appsConfig;
    this.resourceCalculator = resourceCalculator;
    this.nameToCSQueues = nameToCSQueues;
    this.partitionToResource = partitionToResource;
    this.nodeIdToSchedulerNodes = nodeIdToSchedulerNodes;
    init();
  }

  /**
   * Format is:
   * <pre>
   * queueName\t  // app1
   * (priority,resource,host,expression,#repeat,reserved)
   * (priority,resource,host,expression,#repeat,reserved);
   * queueName\t  // app2
   * </pre>
   */
  private void init() {
    int mulp = -1;
    for (String appConfig : config.split(";")) {
      String[] appConfigComponents = appConfig.split("\t");
      String queueName = appConfigComponents[0];
      if (mulp <= 0 && appConfigComponents.length > 2 && appConfigComponents[2] != null) {
        LOG.info("Mulp value: " + appConfigComponents[2]);
        mulp = 100 / (Integer.parseInt(appConfigComponents[2]));
      }

      String containersConfig = appConfigComponents[1];
      MockApplication mockApp = new MockApplication(id, containersConfig, queueName);
      new MockContainers(mockApp, nameToCSQueues, nodeIdToSchedulerNodes);
      add(mockApp);
      id++;
    }
    setupUserResourceUsagePerLabel(resourceCalculator, mulp);
  }

  private void add(MockApplication mockApp) {
    // add to LeafQueue
    LeafQueue queue = (LeafQueue) nameToCSQueues.get(mockApp.queueName);
    queue.getApplications().add(mockApp.app);
    queue.getAllApplications().add(mockApp.app);
    when(queue.getMinimumAllocation()).thenReturn(Resource.newInstance(1,1));
    when(mockApp.app.getCSLeafQueue()).thenReturn(queue);

    LOG.debug("Application mock: queue: " + mockApp.queueName + ", appId:" + mockApp.app);

    Set<String> users = userMap.computeIfAbsent(mockApp.queueName, k -> new HashSet<>());
    users.add(mockApp.app.getUser());

    String label = mockApp.app.getAppAMNodePartitionName();

    // Get label to queue
    Map<String, HashMap<String, ResourceUsage>> userResourceUsagePerQueue =
        userResourceUsagePerLabel.computeIfAbsent(label, k -> new HashMap<>());

    // Get queue to user based resource map
    Map<String, ResourceUsage> userResourceUsage =
        userResourceUsagePerQueue.computeIfAbsent(mockApp.queueName, k -> new HashMap<>());

    // Get user to its resource usage.
    ResourceUsage usage = userResourceUsage.get(mockApp.app.getUser());
    if (null == usage) {
      usage = new ResourceUsage();
      userResourceUsage.put(mockApp.app.getUser(), usage);
    }

    usage.incAMUsed(mockApp.app.getAMResource(label));
    usage.incUsed(mockApp.app.getAppAttemptResourceUsage().getUsed(label));
  }

  private void setupUserResourceUsagePerLabel(ResourceCalculator resourceCalculator,
      int mulp) {
    for (String label : userResourceUsagePerLabel.keySet()) {
      for (String queueName : userMap.keySet()) {
        LeafQueue queue = (LeafQueue) nameToCSQueues.get(queueName);
        // Currently we have user-limit test support only for default label.
        Resource toResourcePartition = partitionToResource.get("");
        Resource capacity = Resources.multiply(toResourcePartition,
            queue.getQueueCapacities().getAbsoluteCapacity());
        Set<String> users = userMap.get(queue.getQueueName());
        //TODO: Refactor this test class to use queue path internally like
        // CS does from now on
        if (users == null) {
          users = userMap.get(queue.getQueuePath());
        }
        when(queue.getAllUsers()).thenReturn(users);
        Resource userLimit = calculateUserLimit(resourceCalculator, mulp, capacity,
            users);
        LOG.debug("Updating user-limit from mock: toResourcePartition="
            + toResourcePartition + ", capacity=" + capacity
            + ", users.size()=" + users.size() + ", userLimit= " + userLimit
            + ",label= " + label + ",queueName= " + queueName);

        setupUserToQueueSettings(label, queueName, queue, users, userLimit);
      }
    }
  }

  private void setupUserToQueueSettings(String label, String queueName,
      LeafQueue queue, Set<String> users, Resource userLimit) {
    Map<String, ResourceUsage> userResourceUsage =
        userResourceUsagePerLabel.get(label).get(queueName);
    for (String userName : users) {
      User user = new User(userName);
      if (userResourceUsage != null) {
        user.setResourceUsage(userResourceUsage.get(userName));
      }
      when(queue.getUser(eq(userName))).thenReturn(user);
      when(queue.getResourceLimitForAllUsers(eq(userName),
          any(Resource.class), anyString(), any(SchedulingMode.class)))
          .thenReturn(userLimit);
    }
  }

  private Resource calculateUserLimit(ResourceCalculator resourceCalculator,
      int mulp, Resource capacity, Set<String> users) {
    if (mulp > 0) {
      return Resources.divideAndCeil(resourceCalculator, capacity, mulp);
    } else {
      return Resources.divideAndCeil(resourceCalculator, capacity, users.size());
    }
  }
}
