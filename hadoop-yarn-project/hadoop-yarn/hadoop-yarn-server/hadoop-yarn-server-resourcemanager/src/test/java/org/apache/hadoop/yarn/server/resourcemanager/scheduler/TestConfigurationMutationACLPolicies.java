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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.QueueAdminConfigurationMutationACLPolicy;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConfigurationMutationACLPolicies {

  private ConfigurationMutationACLPolicy policy;
  private RMContext rmContext;
  private MutableConfScheduler scheduler;

  private static final UserGroupInformation GOOD_USER = UserGroupInformation
      .createUserForTesting("goodUser", new String[] {});
  private static final UserGroupInformation BAD_USER = UserGroupInformation
      .createUserForTesting("badUser", new String[] {});
  private static final Map<String, String> EMPTY_MAP =
      Collections.<String, String>emptyMap();

  @Before
  public void setUp() throws IOException {
    rmContext = mock(RMContext.class);
    scheduler = mock(MutableConfScheduler.class);
    when(rmContext.getScheduler()).thenReturn(scheduler);
    mockQueue("a", scheduler);
    mockQueue("b", scheduler);
    mockQueue("b1", scheduler);
  }

  private void mockQueue(String queueName, MutableConfScheduler scheduler)
      throws IOException {
    QueueInfo queueInfo = QueueInfo.newInstance(queueName, 0, 0, 0, null, null,
        null, null, null, null, false, null, false);
    when(scheduler.getQueueInfo(eq(queueName), anyBoolean(), anyBoolean()))
        .thenReturn(queueInfo);
    Queue queue = mock(Queue.class);
    when(queue.hasAccess(eq(QueueACL.ADMINISTER_QUEUE), eq(GOOD_USER)))
        .thenReturn(true);
    when(queue.hasAccess(eq(QueueACL.ADMINISTER_QUEUE), eq(BAD_USER)))
        .thenReturn(false);
    when(scheduler.getQueue(eq(queueName))).thenReturn(queue);
  }

  @Test
  public void testDefaultPolicy() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, GOOD_USER.getShortUserName());
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
        DefaultConfigurationMutationACLPolicy.class,
        ConfigurationMutationACLPolicy.class);
    policy = ConfigurationMutationACLPolicyFactory.getPolicy(conf);
    policy.init(conf, rmContext);
    assertTrue(policy.isMutationAllowed(GOOD_USER, null));
    assertFalse(policy.isMutationAllowed(BAD_USER, null));
  }
  
  @Test
  public void testQueueAdminBasedPolicy() {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
        QueueAdminConfigurationMutationACLPolicy.class,
        ConfigurationMutationACLPolicy.class);
    policy = ConfigurationMutationACLPolicyFactory.getPolicy(conf);
    policy.init(conf, rmContext);
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    QueueConfigInfo configInfo = new QueueConfigInfo("root.a", EMPTY_MAP);
    updateInfo.getUpdateQueueInfo().add(configInfo);
    assertTrue(policy.isMutationAllowed(GOOD_USER, updateInfo));
    assertFalse(policy.isMutationAllowed(BAD_USER, updateInfo));
  }

  @Test
  public void testQueueAdminPolicyAddQueue() {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
        QueueAdminConfigurationMutationACLPolicy.class,
        ConfigurationMutationACLPolicy.class);
    policy = ConfigurationMutationACLPolicyFactory.getPolicy(conf);
    policy.init(conf, rmContext);
    // Add root.b.b1. Should check ACL of root.b queue.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    QueueConfigInfo configInfo = new QueueConfigInfo("root.b.b2", EMPTY_MAP);
    updateInfo.getAddQueueInfo().add(configInfo);
    assertTrue(policy.isMutationAllowed(GOOD_USER, updateInfo));
    assertFalse(policy.isMutationAllowed(BAD_USER, updateInfo));
  }

  @Test
  public void testQueueAdminPolicyAddNestedQueue() {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
        QueueAdminConfigurationMutationACLPolicy.class,
        ConfigurationMutationACLPolicy.class);
    policy = ConfigurationMutationACLPolicyFactory.getPolicy(conf);
    policy.init(conf, rmContext);
    // Add root.b.b1.b11. Should check ACL of root.b queue.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    QueueConfigInfo configInfo = new QueueConfigInfo("root.b.b2.b21", EMPTY_MAP);
    updateInfo.getAddQueueInfo().add(configInfo);
    assertTrue(policy.isMutationAllowed(GOOD_USER, updateInfo));
    assertFalse(policy.isMutationAllowed(BAD_USER, updateInfo));
  }

  @Test
  public void testQueueAdminPolicyRemoveQueue() {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
        QueueAdminConfigurationMutationACLPolicy.class,
        ConfigurationMutationACLPolicy.class);
    policy = ConfigurationMutationACLPolicyFactory.getPolicy(conf);
    policy.init(conf, rmContext);
    // Remove root.b.b1.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.b.b1");
    assertTrue(policy.isMutationAllowed(GOOD_USER, updateInfo));
    assertFalse(policy.isMutationAllowed(BAD_USER, updateInfo));
  }

  @Test
  public void testQueueAdminPolicyGlobal() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, GOOD_USER.getShortUserName());
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
        QueueAdminConfigurationMutationACLPolicy.class,
        ConfigurationMutationACLPolicy.class);
    policy = ConfigurationMutationACLPolicyFactory.getPolicy(conf);
    policy.init(conf, rmContext);
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    assertTrue(policy.isMutationAllowed(GOOD_USER, updateInfo));
    assertTrue(policy.isMutationAllowed(BAD_USER, updateInfo));
    updateInfo.getGlobalParams().put("globalKey", "globalValue");
    assertTrue(policy.isMutationAllowed(GOOD_USER, updateInfo));
    assertFalse(policy.isMutationAllowed(BAD_USER, updateInfo));
  }
}
