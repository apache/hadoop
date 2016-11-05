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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.QueueACLsTestBase;

public class TestFairSchedulerQueueACLs extends QueueACLsTestBase {
  @Override
  protected Configuration createConfiguration() throws IOException {
    FairSchedulerConfiguration fsConf = new FairSchedulerConfiguration();
    
    final String TEST_DIR = new File(System.getProperty("test.build.data",
        "/tmp")).getAbsolutePath();
    final String ALLOC_FILE = new File(TEST_DIR, "test-queues.xml")
        .getAbsolutePath();
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("  <aclSubmitApps> </aclSubmitApps>");
    out.println("  <aclAdministerApps>root_admin </aclAdministerApps>");
    out.println("  <queue name=\"queueA\">");
    out.println("    <aclSubmitApps>queueA_user,common_user </aclSubmitApps>");
    out.println("    <aclAdministerApps>queueA_admin </aclAdministerApps>");
    out.println("  </queue>");
    out.println("  <queue name=\"queueB\">");
    out.println("    <aclSubmitApps>queueB_user,common_user </aclSubmitApps>");
    out.println("    <aclAdministerApps>queueB_admin </aclAdministerApps>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    fsConf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    fsConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    fsConf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());

    return fsConf;
  }
}
