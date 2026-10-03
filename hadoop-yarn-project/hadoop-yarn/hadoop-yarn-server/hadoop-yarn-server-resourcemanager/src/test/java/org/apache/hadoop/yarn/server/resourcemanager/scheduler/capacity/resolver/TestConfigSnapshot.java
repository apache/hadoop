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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConfigSnapshot {
  private static final String PREFIX = CapacitySchedulerConfiguration.PREFIX;

  @Test
  public void testValuesMatchConfigurationGet() {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(new YarnConfiguration(), false);
    conf.set(PREFIX + "root.queues", "a,b");
    conf.set(PREFIX + "root.a.capacity", "${test.snapshot.capacity}");
    conf.set("test.snapshot.capacity", "40");

    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);

    // yarn-default.xml brings in many substituted values as well
    assertTrue(snapshot.keys().size() > 100);
    for (String key : snapshot.keys()) {
      assertEquals(conf.get(key), snapshot.get(key), key);
    }
  }

  @Test
  public void testVariableSubstitution() {
    Configuration conf = new Configuration(false);
    conf.set("test.snapshot.var", "10");
    conf.set(PREFIX + "root.a.capacity", "${test.snapshot.var}");
    conf.set(PREFIX + "root.a.maximum-capacity", "${test.snapshot.unbound}");
    conf.set(PREFIX + "root.a.state", "RUNNING");

    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);

    assertEquals("10", snapshot.get(PREFIX + "root.a.capacity"));
    assertEquals("${test.snapshot.var}",
        snapshot.getRaw(PREFIX + "root.a.capacity"));
    // An unbound variable is kept literally, like Configuration.get does
    assertEquals("${test.snapshot.unbound}",
        snapshot.get(PREFIX + "root.a.maximum-capacity"));
    assertEquals("RUNNING", snapshot.getRaw(PREFIX + "root.a.state"));

    Map<String, String> expanded =
        snapshot.getPropertiesWithPrefix(PREFIX + "root.a");
    assertEquals("10", expanded.get("capacity"));
    Map<String, String> raw =
        snapshot.getRawPropertiesWithPrefix(PREFIX + "root.a", false);
    assertEquals("${test.snapshot.var}", raw.get("capacity"));
    assertEquals("RUNNING", raw.get("state"));
  }

  @Test
  public void testFailedSubstitutionIsRethrownOnAccess() {
    Configuration conf = new Configuration(false);
    conf.set("test.snapshot.loop.a", "${test.snapshot.loop.b}");
    conf.set("test.snapshot.loop.b", "x${test.snapshot.loop.a}");
    assertThrows(IllegalStateException.class,
        () -> conf.get("test.snapshot.loop.a"));

    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);

    assertTrue(snapshot.keys().contains("test.snapshot.loop.a"));
    assertThrows(IllegalStateException.class,
        () -> snapshot.get("test.snapshot.loop.a"));
    assertEquals("${test.snapshot.loop.b}",
        snapshot.getRaw("test.snapshot.loop.a"));
    assertEquals(2, snapshot
        .getRawPropertiesWithPrefix("test.snapshot.loop", false).size());
  }

  @Test
  public void testDeprecatedKeys() {
    String oldKey = "test.snapshot.deprecated.old";
    String newKey = "test.snapshot.deprecated.new";
    Configuration.addDeprecation(oldKey, newKey);
    Configuration conf = new Configuration(false);
    conf.set(oldKey, "5");

    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);

    assertEquals(conf.get(oldKey), snapshot.get(oldKey));
    assertEquals(conf.get(newKey), snapshot.get(newKey));
    assertEquals("5", snapshot.get(newKey));
    assertTrue(snapshot.keys().contains(newKey));

    conf.set(newKey, "6");
    snapshot = ConfigSnapshot.of(conf);
    assertEquals("6", snapshot.get(oldKey));
    assertEquals("6", snapshot.get(newKey));
  }

  @Test
  public void testAbsentKeys() {
    ConfigSnapshot snapshot = ConfigSnapshot.of(new Configuration(false));

    assertNull(snapshot.get(PREFIX + "root.queues"));
    assertNull(snapshot.getRaw(PREFIX + "root.queues"));
    assertTrue(snapshot.keys().isEmpty());
    assertTrue(snapshot.getPropertiesWithPrefix(PREFIX).isEmpty());
    assertTrue(snapshot.getRawPropertiesWithPrefix(PREFIX, true).isEmpty());
  }

  @Test
  public void testPrefixQueries() {
    Map<String, String> props = new HashMap<>();
    props.put("root.1.2.3", "V1");
    props.put("root.1", "V2");
    props.put("root.1.2", "V3");
    props.put("root.1.2.4", "V31");
    props.put("root.1.2.4.5", "V32");
    props.put("root", "V4");
    props.put("root.12.3", "V5");
    ConfigSnapshot snapshot = ConfigSnapshot.of(props);

    Map<String, String> result = snapshot.getPropertiesWithPrefix("root.1.2");
    assertEquals(4, result.size());
    assertEquals("V3", result.get(""));
    assertEquals("V1", result.get("3"));
    assertEquals("V31", result.get("4"));
    assertEquals("V32", result.get("4.5"));

    // A trailing dot is disregarded, as queue prefixes end with one
    result = snapshot.getPropertiesWithPrefix("root.1.2.4.");
    assertEquals(2, result.size());
    assertEquals("V31", result.get(""));
    assertEquals("V32", result.get("5"));

    // Prefixes match whole key parts only
    result = snapshot.getPropertiesWithPrefix("root.1");
    assertEquals(5, result.size());
    assertFalse(result.containsValue("V5"));
    assertEquals("V5", snapshot.getPropertiesWithPrefix("root.12").get("3"));

    result = snapshot.getPropertiesWithPrefix("root.1.2", true);
    assertEquals(4, result.size());
    assertEquals("V3", result.get("root.1.2"));
    assertEquals("V32", result.get("root.1.2.4.5"));

    assertEquals(7, snapshot.getPropertiesWithPrefix("root").size());
    assertTrue(snapshot.getPropertiesWithPrefix("").isEmpty());
    assertTrue(snapshot.getPropertiesWithPrefix("root.1.2.4.5.6").isEmpty());
    assertTrue(snapshot.getPropertiesWithPrefix("3").isEmpty());
  }

  @Test
  public void testPerLabelPrefixQuery() {
    Configuration conf = new Configuration(false);
    String labels = PREFIX + "root.a.accessible-node-labels";
    conf.set(labels, "x,y");
    conf.set(labels + ".x.capacity", "50");
    conf.set(labels + ".x.maximum-capacity", "100");
    conf.set(labels + ".y.capacity", "20");
    conf.set(PREFIX + "root.a.capacity", "10");

    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);

    Map<String, String> x = snapshot.getPropertiesWithPrefix(labels + ".x.");
    assertEquals(2, x.size());
    assertEquals("50", x.get("capacity"));
    assertEquals("100", x.get("maximum-capacity"));
    Map<String, String> all = snapshot.getPropertiesWithPrefix(labels);
    assertEquals(4, all.size());
    assertEquals("x,y", all.get(""));
  }

  @Test
  public void testImmutability() {
    Configuration conf = new Configuration(false);
    conf.set(PREFIX + "root.queues", "a");
    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);

    conf.set(PREFIX + "root.queues", "a,b");
    conf.set(PREFIX + "root.b.capacity", "50");
    conf.unset(PREFIX + "root.queues");

    assertEquals("a", snapshot.get(PREFIX + "root.queues"));
    assertNull(snapshot.get(PREFIX + "root.b.capacity"));
    assertEquals(1, snapshot.keys().size());
    assertThrows(UnsupportedOperationException.class,
        () -> snapshot.keys().add("x"));

    Map<String, String> result = snapshot.getPropertiesWithPrefix(PREFIX);
    result.put("root.b.capacity", "50");
    assertEquals(1, snapshot.getPropertiesWithPrefix(PREFIX).size());

    Map<String, String> props = new HashMap<>();
    props.put("a.b", "1");
    ConfigSnapshot fromMap = ConfigSnapshot.of(props);
    props.put("a.c", "2");
    assertNull(fromMap.get("a.c"));
  }

  @Test
  public void testCachedOnCapacitySchedulerConfiguration() {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(new Configuration(false), false);
    conf.set(PREFIX + "root.queues", "a");
    ConfigSnapshot snapshot = conf.getConfigSnapshot();

    // Writes after the snapshot was taken are not visible through it
    conf.set(PREFIX + "root.a.capacity", "100");
    assertSame(snapshot, conf.getConfigSnapshot());
    assertNull(conf.getConfigSnapshot().get(PREFIX + "root.a.capacity"));
    assertTrue(conf.getConfigurationProperties()
        .getPropertiesWithPrefix(PREFIX + "root.a").isEmpty());

    conf.reinitializeConfigurationProperties();
    assertEquals("100", conf.getConfigSnapshot().get(PREFIX + "root.a.capacity"));
    assertEquals("100", conf.getConfigurationProperties()
        .getPropertiesWithPrefix(PREFIX + "root.a").get("capacity"));
  }
}
