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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueLabelChecks.AccessibleLabelsInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueLabelChecks.DefaultLabelExpressionInput;
import org.junit.jupiter.api.Test;

public class TestQueueLabelChecks {

  private static Set<String> labels(String... labels) {
    return new LinkedHashSet<>(Arrays.asList(labels));
  }

  @Test
  public void testAccessibleLabelsSubsetPasses() {
    // root is never checked
    assertNull(QueueLabelChecks.checkAccessibleLabelsSubset(
        new AccessibleLabelsInput(true, labels("*"), null)));
    // a parent with "*" or without labels accepts anything
    assertNull(QueueLabelChecks.checkAccessibleLabelsSubset(
        new AccessibleLabelsInput(false, labels("*"), labels("*"))));
    assertNull(QueueLabelChecks.checkAccessibleLabelsSubset(
        new AccessibleLabelsInput(false, labels("x", "y"), labels("x", "*"))));
    assertNull(QueueLabelChecks.checkAccessibleLabelsSubset(
        new AccessibleLabelsInput(false, labels("x"), null)));
    assertNull(QueueLabelChecks.checkAccessibleLabelsSubset(
        new AccessibleLabelsInput(false, labels("x"), labels("x", "y"))));
    assertNull(QueueLabelChecks.checkAccessibleLabelsSubset(
        new AccessibleLabelsInput(false, labels(), labels("x"))));
  }

  @Test
  public void testAccessibleLabelsWildcardUnderRestrictedParent() {
    assertEquals("Parent's accessible queue is not ANY(*), but child's accessible queue is *",
        QueueLabelChecks.checkAccessibleLabelsSubset(
            new AccessibleLabelsInput(false, labels("x", "*"), labels("x"))));
    assertEquals("Parent's accessible queue is not ANY(*), but child's accessible queue is *",
        QueueLabelChecks.checkAccessibleLabelsSubset(
            new AccessibleLabelsInput(false, labels("*"), labels())));
  }

  @Test
  public void testAccessibleLabelsNotSubset() {
    assertEquals("Some labels of child queue is not a subset of parent queue, these labels=[y]",
        QueueLabelChecks.checkAccessibleLabelsSubset(
            new AccessibleLabelsInput(false, labels("x", "y"), labels("x"))));
    assertEquals(
        "Some labels of child queue is not a subset of parent queue, these labels=[y,z]",
        QueueLabelChecks.checkAccessibleLabelsSubset(
            new AccessibleLabelsInput(false, labels("z", "x", "y"), labels("x"))));
  }

  @Test
  public void testDefaultLabelExpressionPasses() {
    assertNull(QueueLabelChecks.checkDefaultLabelExpression(
        new DefaultLabelExpressionInput("root.a", null, null)));
    assertNull(QueueLabelChecks.checkDefaultLabelExpression(
        new DefaultLabelExpressionInput("root.a", labels("x"), "x")));
    assertNull(QueueLabelChecks.checkDefaultLabelExpression(
        new DefaultLabelExpressionInput("root.a", labels("*"), "y")));
    assertNull(QueueLabelChecks.checkDefaultLabelExpression(
        new DefaultLabelExpressionInput("root.a", labels("x", "y"), " x && y ")));
    // an empty expression requests the default partition
    assertNull(QueueLabelChecks.checkDefaultLabelExpression(
        new DefaultLabelExpressionInput("root.a", null, "")));
  }

  @Test
  public void testDefaultLabelExpressionFails() {
    assertEquals("Invalid default label expression of  queue=root.a doesn't have permission"
        + " to access all labels in default label expression. labelExpression of resource"
        + " request=y. Queue labels=x,z",
        QueueLabelChecks.checkDefaultLabelExpression(
            new DefaultLabelExpressionInput("root.a", labels("x", "z"), "y")));
    assertEquals("Invalid default label expression of  queue=root.a doesn't have permission"
        + " to access all labels in default label expression. labelExpression of resource"
        + " request=y. Queue labels=",
        QueueLabelChecks.checkDefaultLabelExpression(
            new DefaultLabelExpressionInput("root.a", null, "y")));
  }

  @Test
  public void testLeafQueueTemplateLabels() {
    assertNull(QueueLabelChecks.checkLeafQueueTemplateLabels("root.m",
        Collections.emptySet(), labels("")));
    assertNull(QueueLabelChecks.checkLeafQueueTemplateLabels("root.m",
        labels("", "x"), labels("", "x", "y")));
    assertEquals("Invalid node label y on configured leaf template on parent queue root.m",
        QueueLabelChecks.checkLeafQueueTemplateLabels("root.m",
            labels("", "y", "z"), labels("", "x")));
  }
}
