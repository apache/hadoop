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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NodeAttributeTestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueTree;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ValueSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Queues are set up from the resolved configuration that the queue context
 * resolves once per (re)initialization, and queues created at runtime are
 * resolved against it.
 */
public class TestCapacitySchedulerResolvedConstruction {

  private MockRM rm;

  @AfterEach
  public void tearDown() {
    if (rm != null) {
      rm.stop();
      rm = null;
    }
    YarnAuthorizationProvider.destroy();
  }

  private static CapacitySchedulerConfiguration configuration() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.set(PREFIX + "root.queues", "a,m,plan");
    csConf.set(PREFIX + "root.a.capacity", "40");
    csConf.set(PREFIX + "root.m.capacity", "40");
    csConf.set(PREFIX + "root.plan.capacity", "20");
    csConf.set(PREFIX + "root.a.queues", "a1");
    csConf.set(PREFIX + "root.a.a1.capacity", "100");
    csConf.set(PREFIX + "root.a.a1.maximum-application-lifetime", "50");
    csConf.set(PREFIX + "root.a.maximum-application-lifetime", "100");
    csConf.set(PREFIX + "root.a.accessible-node-labels", "*");
    // v2 auto queue creation below root.a, which only works with weights
    csConf.setBoolean(PREFIX + "root.a.auto-queue-creation-v2.enabled", true);
    csConf.set(PREFIX + "root.a.auto-queue-creation-v2.template.priority",
        "3");
    csConf.set(PREFIX + "root.a.auto-queue-creation-v2.leaf-template"
        + ".user-limit-factor", "2");
    // A v1 managed parent
    csConf.setBoolean(PREFIX + "root.m.auto-create-child-queue.enabled", true);
    csConf.set(PREFIX + "root.m.leaf-queue-template.capacity", "10");
    csConf.set(PREFIX + "root.m.leaf-queue-template.acl_submit_applications",
        "alice");
    // A plan queue
    csConf.setBoolean(PREFIX + "root.plan.reservable", true);
    csConf.set(PREFIX + "root.plan.user-limit-factor", "3");
    csConf.set(PREFIX + "root.plan.minimum-user-limit-percent", "50");
    return csConf;
  }

  private CapacityScheduler start(CapacitySchedulerConfiguration csConf)
      throws Exception {
    YarnConfiguration conf = NodeAttributeTestUtils.getRandomDirConf(csConf);
    conf.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getName());
    rm = new MockRM(conf);
    rm.start();
    return (CapacityScheduler) rm.getResourceScheduler();
  }

  @Test
  public void testResolvedOncePerRefresh() throws Exception {
    CapacityScheduler cs = start(configuration());
    CapacitySchedulerQueueContext queueContext = cs.getQueueContext();
    ResolvedQueueTree tree = queueContext.getResolvedQueueTree();
    assertNotNull(tree);
    assertEquals(Kind.ROOT, tree.getRoot().getKind());
    assertEquals(Kind.PLAN, tree.get(new QueuePath("root.plan")).getKind());

    // The settings take the inherited values from the resolved tree
    AbstractCSQueue a1 = (AbstractCSQueue) cs.getQueue("root.a.a1");
    ResolvedQueueConfig resolved = tree.get(a1.getQueuePathObject());
    assertEquals(ValueSource.PARENT,
        resolved.get(QueueProperties.ACCESSIBLE_NODE_LABELS).getSource());
    assertEquals(resolved.get(QueueProperties.ACCESSIBLE_NODE_LABELS)
        .getValue(), a1.getAccessibleNodeLabels());
    assertEquals(50L, a1.getMaximumApplicationLifetime());
    assertEquals(50L, a1.getDefaultApplicationLifetime());

    cs.reinitialize(cs.getConfiguration(), rm.getRMContext());
    ResolvedQueueTree refreshed = queueContext.getResolvedQueueTree();
    assertNotSame(tree, refreshed);
    assertSame(cs.getQueue("root.a.a1"), a1);
    assertEquals(refreshed.get(a1.getQueuePathObject())
        .get(QueueProperties.ACCESSIBLE_NODE_LABELS).getValue(),
        a1.getAccessibleNodeLabels());
  }

  @Test
  public void testDynamicQueuesResolvedAgainstInstalledTree()
      throws Exception {
    CapacitySchedulerConfiguration csConf = configuration();
    csConf.setLegacyQueueModeEnabled(false);
    CapacityScheduler cs = start(csConf);
    CapacitySchedulerQueueContext queueContext = cs.getQueueContext();
    ResolvedQueueTree tree = queueContext.getResolvedQueueTree();
    QueuePath parentPath = new QueuePath("root.a.p");
    QueuePath leafPath = new QueuePath("root.a.p.l");
    assertEquals(null, tree.get(parentPath));

    cs.getCapacitySchedulerQueueManager().createQueue(leafPath);
    AbstractLeafQueue leaf = (AbstractLeafQueue) cs.getQueue("root.a.p.l");
    assertNotNull(leaf);
    assertTrue(leaf.isDynamicQueue());

    // The new dynamic parent and leaf are added to the installed tree
    assertSame(tree, queueContext.getResolvedQueueTree());
    ResolvedQueueConfig parent = tree.get(parentPath);
    ResolvedQueueConfig resolvedLeaf = tree.get(leafPath);
    assertEquals(Kind.PARENT, parent.getKind());
    assertTrue(parent.isDynamic());
    assertEquals(Kind.LEAF, resolvedLeaf.getKind());
    assertTrue(resolvedLeaf.isDynamic());
    assertEquals(ValueSource.TEMPLATE_V2,
        parent.get(QueueProperties.PRIORITY).getSource());
    assertEquals(3, cs.getQueue("root.a.p").getPriority().getPriority());
    // The nested leaf takes the dynamic leaf default, not the template of
    // root.a, which only applies to the direct children of root.a
    assertEquals(-1f, leaf.getUserLimitFactor(), 0f);
    assertEquals(resolvedLeaf.get(QueueProperties.USER_LIMIT_FACTOR)
        .getValue(), leaf.getUserLimitFactor(), 0f);

    // A refresh resolves the existing dynamic queues, and keeps the dynamic
    // parent a parent
    cs.reinitialize(cs.getConfiguration(), rm.getRMContext());
    ResolvedQueueTree refreshed = queueContext.getResolvedQueueTree();
    assertNotSame(tree, refreshed);
    assertEquals(Kind.PARENT, refreshed.get(parentPath).getKind());
    assertTrue(refreshed.get(leafPath).isDynamic());
    assertSame(leaf, cs.getQueue("root.a.p.l"));
  }

  @Test
  public void testLegacyAutoCreatedLeafResolvedFromTemplate()
      throws Exception {
    CapacityScheduler cs = start(configuration());
    CapacitySchedulerQueueContext queueContext = cs.getQueueContext();
    QueuePath path = new QueuePath("root.m.u1");

    cs.getCapacitySchedulerQueueManager().createQueue(path);
    AbstractCSQueue u1 = (AbstractCSQueue) cs.getQueue("root.m.u1");
    assertTrue(u1 instanceof AutoCreatedLeafQueue);

    ResolvedQueueConfig resolved =
        queueContext.getResolvedQueueTree().get(path);
    assertEquals(Kind.LEAF, resolved.getKind());
    assertEquals(ValueSource.TEMPLATE_V1,
        resolved.get(QueueProperties.ACL_SUBMIT_APPLICATIONS).getSource());
    assertEquals("alice",
        u1.getACLs().get(AccessType.SUBMIT_APP).getAclString().trim());
    // v1 auto-created leaves are not dynamic queue objects: no 1w default
    assertEquals(ValueSource.TEMPLATE_V1,
        resolved.get(QueueProperties.CAPACITY_VECTOR).getSource());
  }

  @Test
  public void testReservationQueueTakesPlanLimits() throws Exception {
    CapacityScheduler cs = start(configuration());
    CapacitySchedulerQueueContext queueContext = cs.getQueueContext();
    PlanQueue plan = (PlanQueue) cs.getQueue("root.plan");

    ReservationQueue reservation =
        new ReservationQueue(queueContext, "r1", plan);
    ResolvedQueueConfig resolved = queueContext.getResolvedQueueTree()
        .get(reservation.getQueuePathObject());
    assertEquals(Kind.RESERVATION, resolved.getKind());
    assertEquals(ValueSource.PARENT,
        resolved.get(QueueProperties.USER_LIMIT_FACTOR).getSource());
    assertEquals(3f, reservation.getUserLimitFactor(), 0f);
    assertEquals(50f, reservation.getUserLimit(), 0f);
    assertEquals(plan.getMaxApplicationsForReservations(),
        reservation.getMaxApplications());
    assertFalse(reservation.isDynamicQueue());
  }
}
