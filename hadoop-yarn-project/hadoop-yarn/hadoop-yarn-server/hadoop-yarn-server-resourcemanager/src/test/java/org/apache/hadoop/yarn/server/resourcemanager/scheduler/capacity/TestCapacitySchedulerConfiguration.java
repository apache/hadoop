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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getQueuePrefix;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCapacitySchedulerConfiguration {

  private CapacitySchedulerConfiguration createDefaultCsConf() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }

  private AccessControlList getSubmitAcl(CapacitySchedulerConfiguration csConf, String queue) {
    return csConf.getAcl(queue, QueueACL.SUBMIT_APPLICATIONS);
  }

  private void setSubmitAppsConfig(CapacitySchedulerConfiguration csConf, String queue, String value) {
    csConf.set(getSubmitAppsConfigKey(queue), value);
  }

  private String getSubmitAppsConfigKey(String queue) {
    return getQueuePrefix(queue) + "acl_submit_applications";
  }

  @Test
  public void testDefaultSubmitACLForRootAllAllowed() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    AccessControlList acl = csConf.getAcl(ROOT, QueueACL.SUBMIT_APPLICATIONS);
    assertTrue(acl.getUsers().isEmpty());
    assertTrue(acl.getGroups().isEmpty());
    assertTrue(acl.isAllAllowed());
  }

  @Test
  public void testDefaultSubmitACLForRootChildNoneAllowed() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    AccessControlList acl = getSubmitAcl(csConf, ROOT + ".test");
    assertTrue(acl.getUsers().isEmpty());
    assertTrue(acl.getGroups().isEmpty());
    assertFalse(acl.isAllAllowed());
  }

  @Test
  public void testSpecifiedEmptySubmitACLForRoot() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    setSubmitAppsConfig(csConf, ROOT, "");
    AccessControlList acl = csConf.getAcl(ROOT, QueueACL.SUBMIT_APPLICATIONS);
    assertTrue(acl.getUsers().isEmpty());
    assertTrue(acl.getGroups().isEmpty());
    assertFalse(acl.isAllAllowed());
  }

  @Test
  public void testSpecifiedEmptySubmitACLForRootIsNotInherited() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    setSubmitAppsConfig(csConf, ROOT, "");
    AccessControlList acl = getSubmitAcl(csConf, ROOT + ".test");
    assertTrue(acl.getUsers().isEmpty());
    assertTrue(acl.getGroups().isEmpty());
    assertFalse(acl.isAllAllowed());
  }
  
}