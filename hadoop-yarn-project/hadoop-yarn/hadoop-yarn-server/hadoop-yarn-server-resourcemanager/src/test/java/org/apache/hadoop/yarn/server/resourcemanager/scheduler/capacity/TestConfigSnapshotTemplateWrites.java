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
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Pins which readers see the values that v2 templates write into the shared
 * queue context configuration while a dynamic queue is set up. The cached
 * configuration snapshot is taken before any template entry is written, so
 * readers going through it (user weights, the explicitly-set check of the
 * templates) do not see them, while the ordinary getters do.
 */
public class TestConfigSnapshotTemplateWrites {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath A = new QueuePath("root.a");
  private static final QueuePath B = new QueuePath("root.b");
  private static final String AUTO = "root.a.auto1";
  private static final String AUTO_PREFIX =
      CapacitySchedulerConfiguration.PREFIX + AUTO + ".";
  private static final String TEMPLATE =
      AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(A);

  private MockRM mockRM;
  private CapacityScheduler cs;
  private CapacitySchedulerConfiguration csConf;

  @BeforeEach
  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    csConf.setQueues(ROOT, new String[] {"a", "b"});
    csConf.setNonLabeledQueueWeight(ROOT, 1f);
    csConf.setNonLabeledQueueWeight(A, 1f);
    csConf.setNonLabeledQueueWeight(B, 1f);
    csConf.setAutoQueueCreationV2Enabled(A, true);
    csConf.set(TEMPLATE + "user-limit-factor", "3");
    csConf.set(TEMPLATE + "user-settings.u1."
        + CapacitySchedulerConfiguration.USER_WEIGHT, "0.5");

    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mockRM = new MockRM(csConf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    mockRM.start();
    cs.start();
  }

  @AfterEach
  public void tearDown() {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  @Test
  public void testTemplateWritesInvisibleToSnapshotReaders() throws Exception {
    ConfigSnapshot snapshotBefore = cs.getQueueContext().getConfigSnapshot();
    AbstractLeafQueue leaf = cs.getCapacitySchedulerQueueManager()
        .createQueue(new QueuePath(AUTO));
    assertTemplateVisibility(leaf);
    assertSame(snapshotBefore, cs.getQueueContext().getConfigSnapshot());

    // A refresh installs a new snapshot, taken before the dynamic queue is set
    // up again, so the template writes stay invisible to it.
    cs.reinitialize(csConf, mockRM.getRMContext());
    assertNotSame(snapshotBefore, cs.getQueueContext().getConfigSnapshot());
    assertTemplateVisibility((AbstractLeafQueue) cs.getQueue(AUTO));
  }

  private void assertTemplateVisibility(AbstractLeafQueue leaf) {
    CapacitySchedulerConfiguration queueConf =
        cs.getQueueContext().getConfiguration();
    ConfigSnapshot snapshot = cs.getQueueContext().getConfigSnapshot();
    String userWeightKey = AUTO_PREFIX + "user-settings.u1."
        + CapacitySchedulerConfiguration.USER_WEIGHT;

    // The template entries are written into the queue context configuration
    assertEquals("3", queueConf.get(AUTO_PREFIX + "user-limit-factor"));
    assertEquals("0.5", queueConf.get(userWeightKey));
    // but not into its snapshot
    assertSame(snapshot, queueConf.getConfigSnapshot());
    assertNull(snapshot.get(AUTO_PREFIX + "user-limit-factor"));
    assertNull(snapshot.get(userWeightKey));

    // The getter reads the configuration: the template value overrides the
    // dynamic leaf default of -1, which is written before the templates and
    // therefore does not count as explicitly set.
    assertEquals(3f, leaf.getUserLimitFactor(), 1e-6);
    // User weights are read through the snapshot and miss the template value
    assertEquals(UserWeights.DEFAULT_WEIGHT,
        leaf.getUserWeights().getByUser("u1"), 1e-6);
  }
}
