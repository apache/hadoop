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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.QueueACLsTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;

public class TestFairSchedulerQueueACLs extends QueueACLsTestBase {
  @Override
  protected Configuration createConfiguration() {
    FairSchedulerConfiguration fsConf = new FairSchedulerConfiguration();
    
    final String testDir = new File(System.getProperty(
        GenericTestUtils.SYSPROP_TEST_DATA_DIR, "/tmp")).getAbsolutePath();
    final String allocFile = new File(testDir, "test-queues.xml")
        .getAbsolutePath();

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .aclSubmitApps(" ")
            .aclAdministerApps("root_admin ")
            .subQueue(new AllocationFileQueue.Builder("queueA")
                .aclSubmitApps("queueA_user,common_user ")
                .aclAdministerApps("queueA_admin ").build())
            .subQueue(new AllocationFileQueue.Builder("queueB")
                .aclSubmitApps("queueB_user,common_user ")
                .aclAdministerApps("queueB_admin ").build())
            .build())
        .writeToFile(allocFile);

    fsConf.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);

    fsConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    fsConf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());

    return fsConf;
  }

  @Override
  public String getQueueD() {
    return "root." + QUEUED;
  }

  @Override
  public String getQueueD1() {
    return "root."+ QUEUED + "." + QUEUED1;
  }

  /**
   * Creates the following queue hierarchy:
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
    FairSchedulerConfiguration fsConf = (FairSchedulerConfiguration) getConf();
    fsConf.clear();
    final String testDir = new File(System.getProperty(
        GenericTestUtils.SYSPROP_TEST_DATA_DIR, "/tmp")).getAbsolutePath();
    final String allocFile = new File(testDir, "test-queues.xml")
        .getAbsolutePath();

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .aclSubmitApps(rootAcl)
            .aclAdministerApps(rootAcl)
            .subQueue(new AllocationFileQueue.Builder(QUEUED)
                .aclAdministerApps(queueDAcl)
                .aclSubmitApps(queueDAcl)
                .subQueue(new AllocationFileQueue.Builder(QUEUED1)
                    .aclSubmitApps(queueD1Acl)
                    .aclAdministerApps(queueD1Acl)
                    .build())
                .build())
            .build())
        .writeToFile(allocFile);

    fsConf.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);

    fsConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    fsConf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    resourceManager.getResourceScheduler()
        .reinitialize(fsConf, resourceManager.getRMContext());

  }
}
