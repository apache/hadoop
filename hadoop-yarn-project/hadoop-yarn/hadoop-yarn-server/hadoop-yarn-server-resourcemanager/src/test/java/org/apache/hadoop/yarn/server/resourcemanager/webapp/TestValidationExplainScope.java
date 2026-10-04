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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests the explain scope of the validate/v2 endpoint.
 */
public class TestValidationExplainScope {

  private static final String PREFIX = "yarn.scheduler.capacity.";

  /**
   * root: a (a1, a2), b, c (c1 (c11)).
   */
  private static ConfigSnapshot proposed() {
    Configuration conf = new Configuration(false);
    conf.set(PREFIX + "root.queues", "a, b,c");
    conf.set(PREFIX + "root.a.queues", "a1,a2");
    conf.set(PREFIX + "root.c.queues", "c1");
    conf.set(PREFIX + "root.c.c1.queues", "c11");
    conf.set(PREFIX + "root.a.capacity", "50");
    return ConfigSnapshot.of(conf);
  }

  private static QueueConfigInfo queue(String path) {
    return new QueueConfigInfo(path, new HashMap<String, String>());
  }

  private static List<String> affected(SchedConfUpdateInfo mutation) {
    return ValidationExplainScope.affectedQueues(mutation, proposed());
  }

  @Test
  public void testParamAbsentOrBlankMeansNoExplain() {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    assertNull(ValidationExplainScope.resolve(null, mutation, proposed()));
    assertNull(ValidationExplainScope.resolve(" ", mutation, proposed()));
  }

  @Test
  public void testParamCommaSeparatedPaths() {
    assertEquals(Arrays.asList("root.a", "root.b"),
        ValidationExplainScope.resolve(" root.a, root.b,,root.a ",
            new SchedConfUpdateInfo(), proposed()));
    assertEquals(Collections.singletonList("root.missing"),
        ValidationExplainScope.resolve("root.missing",
            new SchedConfUpdateInfo(), proposed()));
  }

  @Test
  public void testParamAffected() {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    mutation.getUpdateQueueInfo().add(queue("root.c.c1"));
    assertEquals(Arrays.asList("root.c.c1", "root.c.c1.c11"),
        ValidationExplainScope.resolve("affected", mutation, proposed()));
  }

  @Test
  public void testEmptyMutationAffectsNothing() {
    assertEquals(Collections.emptyList(), affected(new SchedConfUpdateInfo()));
  }

  @Test
  public void testUpdatedQueueAndDescendants() {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    mutation.getUpdateQueueInfo().add(queue("root.a"));
    mutation.getUpdateQueueInfo().add(queue("root.b"));
    assertEquals(Arrays.asList("root.a", "root.a.a1", "root.a.a2", "root.b"),
        affected(mutation));
  }

  @Test
  public void testAddedQueueAndItsParentOnly() {
    Configuration conf = new Configuration(false);
    conf.set(PREFIX + "root.queues", "a,b");
    conf.set(PREFIX + "root.a.queues", "a1,new");
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    mutation.getAddQueueInfo().add(queue("root.a.new"));
    assertEquals(Arrays.asList("root.a", "root.a.new"),
        ValidationExplainScope.affectedQueues(mutation,
            ConfigSnapshot.of(conf)));
  }

  @Test
  public void testRemovedQueueReportsParentOnly() {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    mutation.getRemoveQueueInfo().add("root.c.gone");
    assertEquals(Collections.singletonList("root.c"), affected(mutation));
  }

  @Test
  public void testQueueScopedGlobalKeyUsesLongestQueue() {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    Map<String, String> global = mutation.getGlobalParams();
    global.put(PREFIX + "root.c.c1.auto-queue-creation-v2.template.capacity",
        "1w");
    assertEquals(Arrays.asList("root.c.c1", "root.c.c1.c11"),
        affected(mutation));
  }

  @Test
  public void testSchedulerWideGlobalKeyAffectsEveryQueue() {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    mutation.getGlobalParams().put(PREFIX + "maximum-applications", "10");
    assertEquals(Arrays.asList("root", "root.a", "root.a.a1", "root.a.a2",
        "root.b", "root.c", "root.c.c1", "root.c.c1.c11"),
        affected(mutation));
  }

  @Test
  public void testUnknownQueuesAreAppendedSorted() {
    SchedConfUpdateInfo mutation = new SchedConfUpdateInfo();
    mutation.getUpdateQueueInfo().add(queue("root.z"));
    mutation.getUpdateQueueInfo().add(queue("root.y"));
    mutation.getUpdateQueueInfo().add(queue("root.b"));
    assertEquals(Arrays.asList("root.b", "root.y", "root.z"),
        affected(mutation));
  }
}
