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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getQueuePrefix;

public class TestCapacitySchedulerConfiguration {

  @Test
  public void testDefaultSubmitACLForRootAllAllowed() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(new Configuration(false), false);
    AccessControlList acl = csConf.getAcl(ROOT, QueueACL.SUBMIT_APPLICATIONS);
    Assert.assertTrue(acl.getUsers().isEmpty());
    Assert.assertTrue(acl.getGroups().isEmpty());
    Assert.assertTrue(acl.isAllAllowed());
  }

  @Test
  public void testDefaultSubmitACLForRootChildNoneAllowed() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(new Configuration(false), false);
    AccessControlList acl = csConf.getAcl(ROOT + ".test", QueueACL.SUBMIT_APPLICATIONS);
    Assert.assertTrue(acl.getUsers().isEmpty());
    Assert.assertTrue(acl.getGroups().isEmpty());
    Assert.assertFalse(acl.isAllAllowed());
  }

  @Test
  public void testSpecifiedEmptySubmitACLForRoot() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(new Configuration(false), false);
    String configKey = getQueuePrefix(ROOT) + "acl_submit_applications";
    csConf.set(configKey, "");
    AccessControlList acl = csConf.getAcl(ROOT, QueueACL.SUBMIT_APPLICATIONS);
    Assert.assertTrue(acl.getUsers().isEmpty());
    Assert.assertTrue(acl.getGroups().isEmpty());
    Assert.assertFalse(acl.isAllAllowed());
  }

  @Test
  public void testSpecifiedEmptySubmitACLForRootIsNotInherited() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(new Configuration(false), false);
    String configKey = getQueuePrefix(ROOT) + "acl_submit_applications";
    csConf.set(configKey, "");
    AccessControlList acl = csConf.getAcl(ROOT + ".test", QueueACL.SUBMIT_APPLICATIONS);
    Assert.assertTrue(acl.getUsers().isEmpty());
    Assert.assertTrue(acl.getGroups().isEmpty());
    Assert.assertFalse(acl.isAllAllowed());
  }
}