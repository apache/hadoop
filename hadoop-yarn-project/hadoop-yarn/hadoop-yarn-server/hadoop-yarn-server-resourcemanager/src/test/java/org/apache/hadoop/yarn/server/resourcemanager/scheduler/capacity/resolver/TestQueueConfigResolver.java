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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs.DynamicQueue;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resolution steps, template mechanics, inheritance and derived values of
 * {@link QueueConfigResolver}, against configurations built in the test.
 */
public class TestQueueConfigResolver {
  private static final String P = CapacitySchedulerConfiguration.PREFIX;

  public static ResolutionInputs inputs(Configuration conf,
      List<DynamicQueue> dynamicQueues) {
    return new ResolutionInputs(dynamicQueues,
        ResourceUtils.fetchMaximumAllocationFromConfig(conf),
        Priority.newInstance(conf.getInt(
            YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
            YarnConfiguration.DEFAULT_CLUSTER_LEVEL_APPLICATION_PRIORITY)));
  }

  static ResolvedQueueTree resolve(Configuration conf,
      DynamicQueue... dynamicQueues) {
    List<DynamicQueue> list = new ArrayList<>();
    Collections.addAll(list, dynamicQueues);
    return QueueConfigResolver.resolve(ConfigSnapshot.of(conf),
        inputs(conf, list));
  }

  private static Configuration conf(String... keyValues) {
    Configuration conf = new Configuration(false);
    for (int i = 0; i < keyValues.length; i += 2) {
      conf.set(P + keyValues[i], keyValues[i + 1]);
    }
    return conf;
  }

  private static QueuePath path(String path) {
    return new QueuePath(path);
  }

  @Test
  public void testQueueGlobalAndDefaultSteps() {
    Configuration conf = conf("root.queues", "a,b", "root.a.capacity", "60",
        "root.b.capacity", "40", "root.a.user-limit-factor", "2",
        "user-limit-factor", "3", "root.a.maximum-am-resource-percent", "0.5");
    ResolvedQueueTree tree = resolve(conf);
    ResolvedQueueConfig a = tree.get(path("root.a"));
    ResolvedQueueConfig b = tree.get(path("root.b"));

    assertEquals(Kind.ROOT, tree.getRoot().getKind());
    assertEquals(Kind.LEAF, a.getKind());
    assertEquals(2, tree.getChildren(path("root")).size());

    assertEquals(2f, a.get(USER_LIMIT_FACTOR).getValue());
    assertEquals(ValueSource.QUEUE, a.get(USER_LIMIT_FACTOR).getSource());
    assertEquals(P + "root.a.user-limit-factor",
        a.get(USER_LIMIT_FACTOR).getSourceDetail());
    assertEquals(3f, b.get(USER_LIMIT_FACTOR).getValue());
    assertEquals(ValueSource.GLOBAL, b.get(USER_LIMIT_FACTOR).getSource());
    assertEquals(100f, b.get(USER_LIMIT).getValue());
    assertEquals(ValueSource.DEFAULT, b.get(USER_LIMIT).getSource());
    // The per-label AM percent falls back to the queue key
    assertEquals(ValueSource.QUEUE,
        a.get(LABELED_MAXIMUM_AM_RESOURCE_PERCENT).getSource());
    assertEquals(0.5f, a.get(LABELED_MAXIMUM_AM_RESOURCE_PERCENT).getValue());

    Map<String, Resolved<?>> explained = a.explain();
    assertSame(a.get(USER_LIMIT_FACTOR),
        explained.get(P + "root.a.user-limit-factor"));
    assertNull(a.get(QUEUE_ORDERING_POLICY));
  }

  @Test
  public void testExplainHasTheUserFacingValueOfEachKey() {
    Configuration conf = conf("root.queues", "pct,w,abs,vec",
        "root.pct.capacity", "40", "root.pct.maximum-capacity", "80",
        "root.w.capacity", "2w",
        "root.abs.capacity", "[memory=1024,vcores=1]",
        "root.abs.maximum-capacity", "[memory=2048,vcores=2]",
        "root.vec.capacity", "[memory=50%,vcores=2w]",
        "root.pct.accessible-node-labels", "x",
        "root.pct.accessible-node-labels.x.capacity", "30",
        "root.pct.accessible-node-labels.x.maximum-capacity", "60");
    ResolvedQueueTree tree = resolve(conf);

    Map<String, Resolved<?>> pct = tree.get(path("root.pct")).explain();
    assertEquals(40f, pct.get(P + "root.pct.capacity").getValue());
    assertEquals(ValueSource.QUEUE,
        pct.get(P + "root.pct.capacity").getSource());
    assertEquals(80f, pct.get(P + "root.pct.maximum-capacity").getValue());
    assertEquals(30f, pct.get(P
        + "root.pct.accessible-node-labels.x.capacity").getValue());
    assertEquals(60f, pct.get(P
        + "root.pct.accessible-node-labels.x.maximum-capacity").getValue());

    Map<String, Resolved<?>> w = tree.get(path("root.w")).explain();
    assertEquals("2.0w", w.get(P + "root.w.capacity").getValue());
    assertEquals(100f, w.get(P + "root.w.maximum-capacity").getValue());
    assertEquals(ValueSource.DEFAULT,
        w.get(P + "root.w.maximum-capacity").getSource());

    Map<String, Resolved<?>> abs = tree.get(path("root.abs")).explain();
    assertEquals(Resource.newInstance(1024, 1),
        abs.get(P + "root.abs.capacity").getValue());
    assertEquals(Resource.newInstance(2048, 2),
        abs.get(P + "root.abs.maximum-capacity").getValue());

    Map<String, Resolved<?>> vec = tree.get(path("root.vec")).explain();
    assertTrue(vec.get(P + "root.vec.capacity").getValue()
        instanceof QueueCapacityVector);
  }

  @Test
  public void testParseFailuresAreKeptAndRethrown() {
    Configuration conf = conf("root.queues", "a,b", "root.a.capacity", "abc",
        "root.b.capacity", "150", "root.b.maximum-allocation", "memory-mb=x");
    ResolvedQueueTree tree = resolve(conf);
    Resolved<Float> a = tree.get(path("root.a")).get(CAPACITY);
    assertTrue(a.isFailed());
    NumberFormatException nfe =
        assertThrows(NumberFormatException.class, a::getValue);
    assertEquals(a.getFailureMessage(), nfe.getMessage());

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf, false);
    IllegalArgumentException getterFailure = assertThrows(
        IllegalArgumentException.class,
        () -> csConf.getNonLabeledQueueCapacity(path("root.b")));
    Resolved<Float> b = tree.get(path("root.b")).get(CAPACITY);
    IllegalArgumentException resolvedFailure =
        assertThrows(IllegalArgumentException.class, b::getValue);
    assertEquals(getterFailure.getMessage(), resolvedFailure.getMessage());
    assertTrue(tree.get(path("root.b")).get(MAXIMUM_ALLOCATION).isFailed());
  }

  @Test
  public void testInheritedValues() {
    Configuration conf = conf("root.queues", "a", "root.a.queues", "a1,a2",
        "root.a.accessible-node-labels", "x",
        "root.a.default-node-label-expression", "x",
        "root.a.maximum-application-lifetime", "100",
        "root.a.a1.maximum-application-lifetime", "50",
        "root.a.a2.default-application-lifetime", "20",
        "root.a.disable_preemption", "true",
        "root.a.ordering-policy", "priority-utilization",
        "root.a.maximum-allocation-mb", "2048",
        "root.a.state", "STOPPED",
        "root.a.user-settings.alice.weight", "2",
        "root.a.a1.user-settings.bob.weight", "3");
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    ResolvedQueueTree tree = resolve(conf);
    ResolvedQueueConfig a = tree.get(path("root.a"));
    ResolvedQueueConfig a1 = tree.get(path("root.a.a1"));
    ResolvedQueueConfig a2 = tree.get(path("root.a.a2"));

    assertEquals(Collections.singleton("x"),
        a1.get(ACCESSIBLE_NODE_LABELS).getValue());
    assertEquals(ValueSource.PARENT, a1.get(ACCESSIBLE_NODE_LABELS).getSource());
    assertEquals("root.a",
        a1.get(ACCESSIBLE_NODE_LABELS).getSourceDetail());
    assertEquals("x", a1.get(DEFAULT_NODE_LABEL_EXPRESSION).getValue());

    assertEquals(50L, (long) a1.get(MAXIMUM_APPLICATION_LIFETIME).getValue());
    assertEquals(100L, (long) a2.get(MAXIMUM_APPLICATION_LIFETIME).getValue());
    // Not specified in the hierarchy: the queue's own maximum lifetime
    assertEquals(50L, (long) a1.get(DEFAULT_APPLICATION_LIFETIME).getValue());
    assertEquals(20L, (long) a2.get(DEFAULT_APPLICATION_LIFETIME).getValue());

    assertTrue(a1.get(PREEMPTION_DISABLED).getValue());
    assertEquals(ValueSource.PARENT, a1.get(PREEMPTION_DISABLED).getSource());
    assertEquals(ValueSource.QUEUE, a.get(PREEMPTION_DISABLED).getSource());
    // The intra-queue preemption gate is off by default
    assertEquals(ValueSource.GLOBAL,
        a1.get(INTRA_QUEUE_PREEMPTION_DISABLED).getSource());

    assertEquals("priority-utilization",
        a.get(QUEUE_ORDERING_POLICY).getValue());
    assertEquals("utilization",
        tree.getRoot().get(QUEUE_ORDERING_POLICY).getValue());

    Resource maxAllocation = a1.get(MAXIMUM_ALLOCATION).getValue();
    assertEquals(2048, maxAllocation.getMemorySize());
    assertEquals(ValueSource.PARENT, a1.get(MAXIMUM_ALLOCATION).getSource());
    assertEquals(ValueSource.GLOBAL,
        tree.getRoot().get(MAXIMUM_ALLOCATION).getSource());

    assertEquals(QueueState.STOPPED, a1.get(STATE).getValue());
    assertEquals(ValueSource.PARENT, a1.get(STATE).getSource());

    Map<String, Float> weights = a1.get(USER_WEIGHTS).getValue();
    assertEquals(2f, weights.get("alice"));
    assertEquals(3f, weights.get("bob"));
  }

  @Test
  public void testDerivedValues() {
    Configuration conf = conf("root.queues", "a,b", "root.a.capacity", "40",
        "root.b.capacity", "60", "root.a.queues", "a1,a2",
        "root.a.a1.capacity", "1w", "root.a.a2.capacity", "3w",
        "root.b.user-limit-factor", "2", "root.b.minimum-user-limit-percent",
        "50", "global-queue-max-application", "-1");
    ResolvedQueueTree tree = resolve(conf);
    assertEquals(0.4f, tree.get(path("root.a")).get(ABSOLUTE_CAPACITY)
        .getValue(), 1e-6);
    assertEquals(0.1f, tree.get(path("root.a.a1")).get(ABSOLUTE_CAPACITY)
        .getValue(), 1e-6);
    assertEquals(0.3f, tree.get(path("root.a.a2")).get(ABSOLUTE_CAPACITY)
        .getValue(), 1e-6);
    ResolvedQueueConfig b = tree.get(path("root.b"));
    assertEquals(6000, (int) b.get(EFFECTIVE_MAXIMUM_APPLICATIONS).getValue());
    assertEquals(6000, (int) b.get(MAXIMUM_APPLICATIONS_PER_USER).getValue());
    assertEquals(ValueSource.DERIVED,
        b.get(EFFECTIVE_MAXIMUM_APPLICATIONS).getSource());
  }

  @Test
  public void testStaticQueuesNeverReceiveTemplates() {
    Configuration conf = conf("root.queues", "a",
        "root.auto-queue-creation-v2.enabled", "true",
        "root.auto-queue-creation-v2.template.maximum-applications", "7",
        "root.auto-queue-creation-v2.template.acl_submit_applications", "u");
    ResolvedQueueTree tree = resolve(conf);
    ResolvedQueueConfig a = tree.get(path("root.a"));
    assertEquals(-1, (int) a.get(MAXIMUM_APPLICATIONS).getValue());
    assertEquals(" ", a.get(ACL_SUBMIT_APPLICATIONS).getValue());
  }

  @Test
  public void testV2TemplatePrecedence() {
    Configuration conf = conf("root.queues", "a", "root.a.queues", "",
        "root.a.auto-queue-creation-v2.enabled", "true",
        "root.a.auto-queue-creation-v2.template.maximum-applications", "1",
        "root.*.auto-queue-creation-v2.leaf-template.maximum-applications",
        "2",
        "root.a.auto-queue-creation-v2.template.priority", "3",
        "root.*.auto-queue-creation-v2.template.priority", "4",
        "root.a.auto-queue-creation-v2.template.user-limit-factor", "5",
        "root.a.auto-queue-creation-v2.template.default-application-priority",
        "6",
        "root.a.auto-queue-creation-v2.template.acl_submit_applications", "u",
        "root.a.d.default-application-priority", "9",
        "root.a.d.maximum-am-resource-percent", "0.3");
    ResolvedQueueTree tree = resolve(conf);
    ResolvedQueueConfig d = QueueConfigResolver.resolveDynamicQueue(tree,
        path("root.a.d"), true);
    assertTrue(d.isDynamic());

    // A type specific entry of a wildcard path beats a generic exact entry
    assertEquals(2, (int) d.get(MAXIMUM_APPLICATIONS).getValue());
    assertEquals(ValueSource.TEMPLATE_V2,
        d.get(MAXIMUM_APPLICATIONS).getSource());
    assertEquals(P + "root.*.auto-queue-creation-v2.leaf-template"
        + ".maximum-applications",
        d.get(MAXIMUM_APPLICATIONS).getSourceDetail());
    // Within one template kind the most specific path wins
    assertEquals(3, (int) d.get(PRIORITY).getValue());
    // Templates override the dynamic leaf defaults
    assertEquals(5f, d.get(USER_LIMIT_FACTOR).getValue());
    // Explicit keys block templates
    assertEquals(9, (int) d.get(DEFAULT_APPLICATION_PRIORITY).getValue());
    assertEquals(ValueSource.QUEUE,
        d.get(DEFAULT_APPLICATION_PRIORITY).getSource());
    // and are overwritten by the dynamic leaf defaults
    assertEquals(1f, d.get(MAXIMUM_AM_RESOURCE_PERCENT).getValue());
    assertEquals(ValueSource.DEFAULT,
        d.get(MAXIMUM_AM_RESOURCE_PERCENT).getSource());
    assertEquals("u", d.get(ACL_SUBMIT_APPLICATIONS).getValue());
    assertEquals(" ", d.get(ACL_ADMINISTER_QUEUE).getValue());

    ResolvedQueueConfig dp = QueueConfigResolver.resolveDynamicQueue(tree,
        path("root.a.dp"), false);
    assertEquals(Kind.PARENT, dp.getKind());
    assertEquals(3, (int) dp.get(PRIORITY).getValue());
    assertNull(dp.get(USER_LIMIT_FACTOR));
    // An existing dynamic queue resolves the same way
    ResolvedQueueConfig existing = resolve(conf,
        new DynamicQueue(path("root.a.d"), false, true)).get(path("root.a.d"));
    assertEquals(d.explain().toString(), existing.explain().toString());
  }

  @Test
  public void testV1TemplateOverwritesExplicitKeys() {
    Configuration conf = conf("root.queues", "m",
        "root.m.auto-create-child-queue.enabled", "true",
        "root.m.leaf-queue-template.capacity", "10",
        "root.m.leaf-queue-template.user-limit-factor", "2",
        "root.m.leaf-queue-template.acl_submit_applications", "${who}",
        "root.m.u1.user-limit-factor", "9",
        "root.m.u1.priority", "4");
    conf.set("who", "bob");
    ResolvedQueueTree tree = resolve(conf,
        new DynamicQueue(path("root.m.u1"), true, true));
    ResolvedQueueConfig u1 = tree.get(path("root.m.u1"));
    assertNotNull(u1);
    assertEquals(2f, u1.get(USER_LIMIT_FACTOR).getValue());
    assertEquals(ValueSource.TEMPLATE_V1, u1.get(USER_LIMIT_FACTOR).getSource());
    assertEquals(10f, u1.get(CAPACITY).getValue());
    assertEquals(4, (int) u1.get(PRIORITY).getValue());
    // v1 does not write the dynamic leaf defaults
    assertEquals(0.1f, u1.get(MAXIMUM_AM_RESOURCE_PERCENT).getValue());
    // v1 ACLs use the substituted template value
    assertEquals("bob", u1.get(ACL_SUBMIT_APPLICATIONS).getValue());
  }

  @Test
  public void testLegacyAutoCreatedLeafHasNoDynamicDefaultVectors() {
    Configuration conf = conf("root.queues", "m,a", "root.m.capacity", "50",
        "root.a.capacity", "50", "root.m.auto-create-child-queue.enabled",
        "true", "root.a.auto-queue-creation-v2.enabled", "true");
    ResolvedQueueTree tree = resolve(conf);
    // A v1 auto-created leaf is not a dynamic queue object: its vectors are
    // read from its path, unlike the 1w and 100% defaults of v2 queues
    ResolvedQueueConfig u1 = QueueConfigResolver.resolveDynamicQueue(tree,
        path("root.m.u1"), true);
    assertTrue(u1.get(CAPACITY_VECTOR).getValue().isEmpty());
    assertEquals(ValueSource.DEFAULT, u1.get(CAPACITY_VECTOR).getSource());
    ResolvedQueueConfig d = QueueConfigResolver.resolveDynamicQueue(tree,
        path("root.a.d"), true);
    assertEquals("dynamic queue default",
        d.get(CAPACITY_VECTOR).getSourceDetail());
  }
}
