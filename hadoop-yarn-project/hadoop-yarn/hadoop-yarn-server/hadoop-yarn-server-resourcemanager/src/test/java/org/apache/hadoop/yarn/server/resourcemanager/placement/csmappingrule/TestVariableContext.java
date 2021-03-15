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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class TestVariableContext {

  @Test
  public void testAddAndGet() {
    VariableContext variables = new VariableContext();

    assertEquals("", variables.get("%user"));
    assertFalse(variables.containsKey("%user"));

    variables.put("%user", "john");
    variables.put("%group", "primary");
    variables.put("%group", "secondary");
    variables.put("%empty", null);
    assertTrue(variables.containsKey("%user"));
    assertTrue(variables.containsKey("%empty"));

    assertEquals("john", variables.get("%user"));
    assertEquals("secondary", variables.get("%group"));
    assertEquals("", variables.get("%empty"));
  }

  @Test(expected = IllegalStateException.class)
  public void testImmutablesCanOnlySetOnceFromSet() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%user", "%primary_group", "%secondary_group");

    variables.setImmutables(immutables);
    variables.setImmutables(immutables);
  }

  @Test(expected = IllegalStateException.class)
  public void testImmutablesCanOnlySetOnceFromArray() {
    VariableContext variables = new VariableContext();

    variables.setImmutables("%user", "%primary_group", "%secondary_group");
    variables.setImmutables("%user", "%primary_group", "%secondary_group");
  }

  @Test(expected = IllegalStateException.class)
  public void testImmutablesCanOnlySetOnceFromSetAndArray() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%user", "%primary_group", "%secondary_group");

    variables.setImmutables(immutables);
    variables.setImmutables("%user", "%primary_group", "%secondary_group");
  }

  @Test
  public void testImmutableVariableCanBeSetOnce() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%user", "%primary_group", "%secondary_group");

    variables.setImmutables(immutables);
    variables.put("%user", "bob");
  }

  @Test(expected = IllegalStateException.class)
  public void testImmutableVariableProtection() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%user", "%primary_group", "%secondary_group");

    variables.setImmutables(immutables);
    variables.put("%user", "bob");
    variables.put("%user", "bob");
  }

  @Test
  public void testAddAndGetWithImmutables() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%user", "%primary_group", "%secondary_group");

    assertFalse(variables.isImmutable("%user"));
    assertFalse(variables.isImmutable("%primary_group"));
    assertFalse(variables.isImmutable("%secondary_group"));
    assertFalse(variables.isImmutable("%default"));

    variables.setImmutables(immutables);
    assertTrue(variables.isImmutable("%user"));
    assertTrue(variables.isImmutable("%primary_group"));
    assertTrue(variables.isImmutable("%secondary_group"));
    assertFalse(variables.isImmutable("%default"));
    variables.put("%user", "bob");
    variables.put("%primary_group", "primary");
    variables.put("%default", "root.default");

    assertEquals("bob", variables.get("%user"));
    assertEquals("primary", variables.get("%primary_group"));
    assertEquals("root.default", variables.get("%default"));

    variables.put("%default", "root.new.default");
    assertEquals("root.new.default", variables.get("%default"));
  }

  @Test
  public void testPathPartReplace() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%user", "%primary_group", "%secondary_group");

    variables
        .setImmutables(immutables)
        .put("%user", "bob")
        .put("%primary_group", "developers")
        .put("%secondary_group", "yarn-dev")
        .put("%default", "default.path")
        .put("%null", null)
        .put("%empty", "");

    HashMap<String, String> testCases = new HashMap<>();
    testCases.put("nothing_to_replace", "nothing_to_replace");
    testCases.put(null, null);
    testCases.put("", "");
    testCases.put("%empty", "");
    testCases.put("%null", "");
    testCases.put("%user", "bob");
    testCases.put("root.regular.path", "root.regular.path");
    testCases.put("root.%empty.path", "root..path");
    testCases.put("root.%empty%empty.path", "root.%empty%empty.path");
    testCases.put("root.%null.path", "root..path");
    testCases.put(
        "root.%user.%primary_group.%secondary_group.%default.%null.%empty.end",
        "root.bob.developers.yarn-dev.default.path...end");
    testCases.put(
        "%user%default.%user.%default", "%user%default.bob.default.path");

    testCases.forEach(
        (k, v) -> assertEquals(v, variables.replacePathVariables(k)));
  }

  @Test
  public void testVariableReplace() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%user", "%primary_group", "%secondary_group");

    variables
        .setImmutables(immutables)
        .put("%user", "bob")
        .put("%userPhone", "555-3221")
        .put("%primary_group", "developers")
        .put("%secondary_group", "yarn-dev")
        .put("%default", "default.path")
        .put("%null", null)
        .put("%empty", "");

    HashMap<String, String> testCases = new HashMap<>();
    testCases.put("nothing_to_replace", "nothing_to_replace");
    testCases.put(null, null);
    testCases.put("", "");
    testCases.put("%empty", "");
    testCases.put("%null", "");
    testCases.put("%user", "bob");
    testCases.put("%userPhone", "555-3221");
    testCases.put("root.regular.path", "root.regular.path");
    testCases.put("root.%empty.path", "root..path");
    testCases.put("root.%empty%empty.path", "root..path");
    testCases.put("root.%null.path", "root..path");
    testCases.put(
        "root.%user.%primary_group.%secondary_group.%default.%null.%empty.end",
        "root.bob.developers.yarn-dev.default.path...end");
    testCases.put(
        "%user%default.%user.%default", "bobdefault.path.bob.default.path");
    testCases.put(
        "userPhoneof%useris%userPhone", "userPhoneofbobis555-3221");

    testCases.forEach((pattern, expected) ->
        assertEquals(expected, variables.replaceVariables(pattern)));
  }

  @Test
  public void testCollectionStore() {
    VariableContext variables = new VariableContext();
    Set<String> coll1 = new HashSet<>();
    Set<String> coll2 = new HashSet<>();

    coll1.add("Bob");
    coll1.add("Roger");
    coll2.add("Bob");

    variables.putExtraDataset("set", coll1);
    variables.putExtraDataset("sameset", coll1);
    variables.putExtraDataset("list", coll2);

    try {
      variables.putExtraDataset("set", coll1);
      fail("Same name cannot be used multiple times to add collections");
    } catch (IllegalStateException e) {
      //Exception expected
    }

    assertSame(coll1, variables.getExtraDataset("set"));
    assertSame(coll1, variables.getExtraDataset("sameset"));
    assertSame(coll2, variables.getExtraDataset("list"));
    assertNull(variables.getExtraDataset("Nothing"));
  }

}
