/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE;

public class TestAutoCreatedQueueTemplate {
  private static final QueuePath TEST_QUEUE_ABC = new QueuePath("root.a.b.c");
  private static final QueuePath TEST_QUEUE_AB = new QueuePath("root.a.b");
  private static final QueuePath TEST_QUEUE_A = new QueuePath("root.a");
  private static final QueuePath TEST_QUEUE_B = new QueuePath("root.b");
  private static final String ROOT = "root";

  private CapacitySchedulerConfiguration conf;

  @Before
  public void setUp() throws Exception {
    conf = new CapacitySchedulerConfiguration();
    conf.setQueues("root", new String[]{"a"});
    conf.setQueues("a", new String[]{"b"});
    conf.setQueues("b", new String[]{"c"});

  }

  @Test
  public void testNonWildCardTemplate() {
    conf.set(getTemplateKey(TEST_QUEUE_AB.getFullPath(), "capacity"), "6w");
    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, TEST_QUEUE_AB);
    template.setTemplateEntriesForChild(conf, TEST_QUEUE_ABC.getFullPath());

    Assert.assertEquals("weight is not set", 6f,
        conf.getNonLabeledQueueWeight(TEST_QUEUE_ABC.getFullPath()), 10e-6);

  }

  @Test
  public void testOneLevelWildcardTemplate() {
    conf.set(getTemplateKey("root.a.*", "capacity"), "6w");
    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, TEST_QUEUE_AB);
    template.setTemplateEntriesForChild(conf, TEST_QUEUE_ABC.getFullPath());

    Assert.assertEquals("weight is not set", 6f,
        conf.getNonLabeledQueueWeight(TEST_QUEUE_ABC.getFullPath()), 10e-6);

  }

  @Test
  public void testTwoLevelWildcardTemplate() {
    conf.set(getTemplateKey("root.*", "capacity"), "6w");
    conf.set(getTemplateKey("root.*.*", "capacity"), "5w");

    new AutoCreatedQueueTemplate(conf, TEST_QUEUE_A)
            .setTemplateEntriesForChild(conf, TEST_QUEUE_AB.getFullPath());
    new AutoCreatedQueueTemplate(conf, TEST_QUEUE_AB)
            .setTemplateEntriesForChild(conf, TEST_QUEUE_ABC.getFullPath());

    Assert.assertEquals("weight is not set", 6f,
            conf.getNonLabeledQueueWeight(TEST_QUEUE_AB.getFullPath()), 10e-6);
    Assert.assertEquals("weight is not set", 5f,
            conf.getNonLabeledQueueWeight(TEST_QUEUE_ABC.getFullPath()), 10e-6);
  }

  @Test
  public void testIgnoredWhenRootWildcarded() {
    conf.set(getTemplateKey("*", "capacity"), "6w");
    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, new QueuePath(ROOT));
    template.setTemplateEntriesForChild(conf, TEST_QUEUE_A.getFullPath());

    Assert.assertEquals("weight is set", -1f,
        conf.getNonLabeledQueueWeight(TEST_QUEUE_A.getFullPath()), 10e-6);
  }

  @Test
  public void testIgnoredWhenNoParent() {
    conf.set(getTemplateKey("root", "capacity"), "6w");
    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, new QueuePath(ROOT));
    template.setTemplateEntriesForChild(conf, ROOT);

    Assert.assertEquals("weight is set", -1f,
        conf.getNonLabeledQueueWeight(ROOT), 10e-6);
  }

  @Test
  public void testWildcardAfterRoot() {
    conf.set(getTemplateKey("root.*", "acl_submit_applications"), "user");
    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, new QueuePath("root.a"));
    template.setTemplateEntriesForChild(conf, "root.a");

    Assert.assertEquals("acl_submit_applications is set", "user",
        template.getTemplateProperties().get("acl_submit_applications"));
  }

  @Test
  public void testTemplatePrecedence() {
    conf.set(getTemplateKey("root.a.b", "capacity"), "6w");
    conf.set(getTemplateKey("root.a.*", "capacity"), "4w");
    conf.set(getTemplateKey("root.*.*", "capacity"), "2w");

    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, TEST_QUEUE_AB);
    template.setTemplateEntriesForChild(conf, TEST_QUEUE_ABC.getFullPath());

    Assert.assertEquals(
        "explicit template does not have the highest precedence", 6f,
        conf.getNonLabeledQueueWeight(TEST_QUEUE_ABC.getFullPath()), 10e-6);

    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration();
    newConf.set(getTemplateKey("root.a.*", "capacity"), "4w");
    template =
        new AutoCreatedQueueTemplate(newConf, TEST_QUEUE_AB);
    template.setTemplateEntriesForChild(newConf, TEST_QUEUE_ABC.getFullPath());

    Assert.assertEquals("precedence is invalid", 4f,
        newConf.getNonLabeledQueueWeight(TEST_QUEUE_ABC.getFullPath()), 10e-6);
  }

  @Test
  public void testRootTemplate() {
    conf.set(getTemplateKey("root", "capacity"), "2w");

    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, new QueuePath(ROOT));
    template.setTemplateEntriesForChild(conf, TEST_QUEUE_A.getFullPath());
    Assert.assertEquals("root property is not set", 2f,
        conf.getNonLabeledQueueWeight(TEST_QUEUE_A.getFullPath()), 10e-6);
  }

  @Test
  public void testQueueSpecificTemplates() {
    conf.set(getTemplateKey("root", "capacity"), "2w");
    conf.set(getLeafTemplateKey("root",
        "default-node-label-expression"), "test");
    conf.set(getLeafTemplateKey("root", "capacity"), "10w");
    conf.setBoolean(getParentTemplateKey(
        "root", AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE), false);

    AutoCreatedQueueTemplate template =
        new AutoCreatedQueueTemplate(conf, new QueuePath(ROOT));
    template.setTemplateEntriesForChild(conf, TEST_QUEUE_A.getFullPath());
    template.setTemplateEntriesForChild(conf, TEST_QUEUE_B.getFullPath(), true);

    Assert.assertNull("default-node-label-expression is set for parent",
        conf.getDefaultNodeLabelExpression(TEST_QUEUE_A.getFullPath()));
    Assert.assertEquals("default-node-label-expression is not set for leaf",
        "test", conf.getDefaultNodeLabelExpression(TEST_QUEUE_B.getFullPath()));
    Assert.assertFalse("auto queue removal is not disabled for parent",
        conf.isAutoExpiredDeletionEnabled(TEST_QUEUE_A.getFullPath()));
    Assert.assertEquals("weight should not be overridden when set by " +
            "queue type specific template",
        10f, conf.getNonLabeledQueueWeight(TEST_QUEUE_B.getFullPath()), 10e-6);
    Assert.assertEquals("weight should be set by common template",
        2f, conf.getNonLabeledQueueWeight(TEST_QUEUE_A.getFullPath()), 10e-6);

  }

  private String getTemplateKey(String queuePath, String entryKey) {
    return CapacitySchedulerConfiguration.getQueuePrefix(queuePath)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX + entryKey;
  }

  private String getParentTemplateKey(String queuePath, String entryKey) {
    return CapacitySchedulerConfiguration.getQueuePrefix(queuePath)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + entryKey;
  }

  private String getLeafTemplateKey(String queuePath, String entryKey) {
    return CapacitySchedulerConfiguration.getQueuePrefix(queuePath)
        + AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + entryKey;
  }
}