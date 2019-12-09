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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Priority;
import org.junit.Assert;
import org.junit.Test;


public class TestApplicationPriorityACLConfiguration {

  private final int defaultPriorityQueueA = 3;
  private final int defaultPriorityQueueB = -1;
  private final int maxPriorityQueueA = 5;
  private final int maxPriorityQueueB = 10;
  private final int clusterMaxPriority = 10;

  private static final String QUEUE_A_USER = "queueA_user";
  private static final String QUEUE_B_USER = "queueB_user";
  private static final String QUEUE_A_GROUP = "queueA_group";

  private static final String QUEUEA = "queueA";
  private static final String QUEUEB = "queueB";
  private static final String QUEUEC = "queueC";

  @Test
  public void testSimpleACLConfiguration() throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB, QUEUEC});

    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA, 50f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB, 25f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEC, 25f);

    // Success case: Configure one user/group level priority acl for queue A.
    String[] aclsForA = new String[2];
    aclsForA[0] = QUEUE_A_USER;
    aclsForA[1] = QUEUE_A_GROUP;
    csConf.setPriorityAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA,
        Priority.newInstance(maxPriorityQueueA),
        Priority.newInstance(defaultPriorityQueueA), aclsForA);

    // Try to get the ACL configs and make sure there are errors/exceptions
    List<AppPriorityACLGroup> pGroupA = csConf.getPriorityAcls(
        CapacitySchedulerConfiguration.ROOT + "." + QUEUEA,
        Priority.newInstance(clusterMaxPriority));

    // Validate!
    verifyACLs(pGroupA, QUEUE_A_USER, QUEUE_A_GROUP, maxPriorityQueueA,
        defaultPriorityQueueA);
  }

  @Test
  public void testACLConfigurationForInvalidCases() throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB, QUEUEC});

    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA, 50f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB, 25f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEC, 25f);

    // Success case: Configure one user/group level priority acl for queue A.
    String[] aclsForA = new String[2];
    aclsForA[0] = QUEUE_A_USER;
    aclsForA[1] = QUEUE_A_GROUP;
    csConf.setPriorityAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA,
        Priority.newInstance(maxPriorityQueueA),
        Priority.newInstance(defaultPriorityQueueA), aclsForA);

    String[] aclsForB = new String[1];
    aclsForB[0] = QUEUE_B_USER;
    csConf.setPriorityAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB,
        Priority.newInstance(maxPriorityQueueB),
        Priority.newInstance(defaultPriorityQueueB), aclsForB);

    // Try to get the ACL configs and make sure there are errors/exceptions
    List<AppPriorityACLGroup> pGroupA = csConf.getPriorityAcls(
        CapacitySchedulerConfiguration.ROOT + "." + QUEUEA,
        Priority.newInstance(clusterMaxPriority));
    List<AppPriorityACLGroup> pGroupB = csConf.getPriorityAcls(
        CapacitySchedulerConfiguration.ROOT + "." + QUEUEB,
        Priority.newInstance(clusterMaxPriority));

    // Validate stored ACL values with configured ones.
    verifyACLs(pGroupA, QUEUE_A_USER, QUEUE_A_GROUP, maxPriorityQueueA,
        defaultPriorityQueueA);
    verifyACLs(pGroupB, QUEUE_B_USER, "", maxPriorityQueueB, 0);
  }

  private void verifyACLs(List<AppPriorityACLGroup> pGroup, String queueUser,
      String queueGroup, int maxPriority, int defaultPriority) {
    AppPriorityACLGroup group = pGroup.get(0);
    String aclString = queueUser + " " + queueGroup;

    Assert.assertEquals(aclString.trim(),
        group.getACLList().getAclString().trim());
    Assert.assertEquals(maxPriority, group.getMaxPriority().getPriority());
    Assert.assertEquals(defaultPriority,
        group.getDefaultPriority().getPriority());
  }
}
