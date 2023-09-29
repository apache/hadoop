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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.QueueACLsTestBase;
import org.junit.Test;

public class TestCapacitySchedulerQueueACLs extends QueueACLsTestBase {
  @Override
  protected Configuration createConfiguration() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {
        QUEUEA, QUEUEB });

    setQueueCapacity(csConf, 50,
        CapacitySchedulerConfiguration.ROOT + "." + QUEUEA);
    setQueueCapacity(csConf, 50,
        CapacitySchedulerConfiguration.ROOT + "." + QUEUEB);

    Map<QueueACL, AccessControlList> aclsOnQueueA =
        new HashMap<QueueACL, AccessControlList>();
    AccessControlList submitACLonQueueA = new AccessControlList(QUEUE_A_USER);
    submitACLonQueueA.addUser(COMMON_USER);
    AccessControlList adminACLonQueueA = new AccessControlList(QUEUE_A_ADMIN);
    aclsOnQueueA.put(QueueACL.SUBMIT_APPLICATIONS, submitACLonQueueA);
    aclsOnQueueA.put(QueueACL.ADMINISTER_QUEUE, adminACLonQueueA);
    csConf.setAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA,
      aclsOnQueueA);

    Map<QueueACL, AccessControlList> aclsOnQueueB =
        new HashMap<QueueACL, AccessControlList>();
    AccessControlList submitACLonQueueB = new AccessControlList(QUEUE_B_USER);
    submitACLonQueueB.addUser(COMMON_USER);
    AccessControlList adminACLonQueueB = new AccessControlList(QUEUE_B_ADMIN);
    aclsOnQueueB.put(QueueACL.SUBMIT_APPLICATIONS, submitACLonQueueB);
    aclsOnQueueB.put(QueueACL.ADMINISTER_QUEUE, adminACLonQueueB);
    csConf.setAcls(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB,
      aclsOnQueueB);

    Map<QueueACL, AccessControlList> aclsOnRootQueue =
        new HashMap<QueueACL, AccessControlList>();
    AccessControlList submitACLonRoot = new AccessControlList("");
    AccessControlList adminACLonRoot = new AccessControlList(ROOT_ADMIN);
    aclsOnRootQueue.put(QueueACL.SUBMIT_APPLICATIONS, submitACLonRoot);
    aclsOnRootQueue.put(QueueACL.ADMINISTER_QUEUE, adminACLonRoot);
    csConf.setAcls(CapacitySchedulerConfiguration.ROOT, aclsOnRootQueue);

    csConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    csConf.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getName());

    return csConf;
  }

  @Override
  public String getQueueD() {
    return QUEUED;
  }

  @Override
  public String getQueueD1() {
    return QUEUED1;
  }

  /**
   * Updates the configuration with the following queue hierarchy:
   * root
   *    |
   *    D
   *    |
   *    D1.
   * @param rootAcl administer queue and submit application ACL for root queue
   * @param queueDAcl administer queue and submit application ACL for D queue
   * @param queueD1Acl administer queue and submit application ACL for D1 queue
   * @throws IOException
   */
  @Override
  public void updateConfigWithDAndD1Queues(String rootAcl, String queueDAcl,
              String queueD1Acl) throws IOException {
    CapacitySchedulerConfiguration csConf =
        (CapacitySchedulerConfiguration) getConf();
    csConf.clear();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {QUEUED, QUEUEA, QUEUEB});

    String dPath = CapacitySchedulerConfiguration.ROOT + "." + QUEUED;
    String d1Path = dPath + "." + QUEUED1;
    csConf.setQueues(dPath, new String[] {QUEUED1});
    setQueueCapacity(csConf, 100, d1Path);
    setQueueCapacity(csConf, 30, CapacitySchedulerConfiguration.ROOT
                                     + "." + QUEUEA);
    setQueueCapacity(csConf, 50, CapacitySchedulerConfiguration.ROOT
                                     + "." + QUEUEB);
    setQueueCapacity(csConf, 20, dPath);

    if (rootAcl != null) {
      setAdminAndSubmitACL(csConf, rootAcl,
          CapacitySchedulerConfiguration.ROOT);
    }

    if (queueDAcl != null) {
      setAdminAndSubmitACL(csConf, queueDAcl, dPath);
    }

    if (queueD1Acl != null) {
      setAdminAndSubmitACL(csConf, queueD1Acl, d1Path);
    }
    resourceManager.getResourceScheduler()
        .reinitialize(csConf, resourceManager.getRMContext());
  }


  private void setQueueCapacity(CapacitySchedulerConfiguration csConf,
               float capacity, String queuePath) {
    csConf.setCapacity(queuePath, capacity);
  }

  private void setAdminAndSubmitACL(CapacitySchedulerConfiguration csConf,
               String queueAcl, String queuePath) {
    csConf.setAcl(queuePath, QueueACL.ADMINISTER_QUEUE, queueAcl);
    csConf.setAcl(queuePath, QueueACL.SUBMIT_APPLICATIONS, queueAcl);
  }

  @Test
  public void testCheckAccessForUserWithOnlyLeafNameProvided() {
    testCheckAccess(false, "dynamicQueue");
  }

  @Test
  public void testCheckAccessForUserWithFullPathProvided() {
    testCheckAccess(true, "root.users.dynamicQueue");
  }

  @Test
  public void testCheckAccessForRootQueue() {
    testCheckAccess(false, "root");
  }

  private void testCheckAccess(boolean expectedResult, String queueName) {
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);
    CSQueue root = mock(ParentQueue.class);
    CSQueue users = mock(ManagedParentQueue.class);
    when(qm.getQueue("root")).thenReturn(root);
    when(qm.getQueue("root.users")).thenReturn(users);
    when(users.hasAccess(any(QueueACL.class),
        any(UserGroupInformation.class))).thenReturn(true);
    UserGroupInformation mockUGI = mock(UserGroupInformation.class);

    CapacityScheduler cs =
        (CapacityScheduler) resourceManager.getResourceScheduler();
    cs.setQueueManager(qm);

    assertEquals("checkAccess() failed", expectedResult,
        cs.checkAccess(mockUGI, QueueACL.ADMINISTER_QUEUE, queueName));
  }
}
