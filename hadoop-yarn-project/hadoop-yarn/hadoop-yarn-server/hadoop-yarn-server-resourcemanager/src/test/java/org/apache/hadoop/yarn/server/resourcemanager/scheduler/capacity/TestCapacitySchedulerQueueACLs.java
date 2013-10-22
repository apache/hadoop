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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.QueueACLsTestBase;

public class TestCapacitySchedulerQueueACLs extends QueueACLsTestBase {
  @Override
  protected Configuration createConfiguration() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {
        QUEUEA, QUEUEB });

    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEA, 50f);
    csConf.setCapacity(CapacitySchedulerConfiguration.ROOT + "." + QUEUEB, 50f);

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
    csConf.set("yarn.resourcemanager.scheduler.class", CapacityScheduler.class.getName());

    return csConf;
  }
}
