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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult.ExplainEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the validation contract types.
 */
public class TestValidationResult {

  @Test
  public void testValidity() {
    assertTrue(ValidationResult.valid().isValid());
    assertTrue(new ValidationResult(Collections.singletonList(
        ValidationIssue.warning(null, null, "r", "m"))).isValid());
    assertFalse(new ValidationResult(Arrays.asList(
        ValidationIssue.warning(null, null, "r", "m"),
        ValidationIssue.error("root.a", "k", "r", "m"))).isValid());
  }

  @Test
  public void testIssuesAreImmutableCopies() {
    List<ValidationIssue> issues = new ArrayList<>();
    issues.add(ValidationIssue.warning(null, null, "r", "m"));
    ValidationResult result = new ValidationResult(issues);
    issues.add(ValidationIssue.error(null, null, "r", "m"));
    assertEquals(1, result.getIssues().size());
    assertThrows(UnsupportedOperationException.class,
        () -> result.getIssues().clear());
  }

  @Test
  public void testExplain() {
    ValidationResult result = ValidationResult.valid();
    assertFalse(result.hasExplain());
    assertTrue(result.getExplain().isEmpty());

    Map<String, List<ExplainEntry>> explain = new LinkedHashMap<>();
    explain.put("root.b", Collections.singletonList(
        new ExplainEntry("k.b", "1", "DEFAULT", null)));
    explain.put("root.a", Collections.singletonList(
        new ExplainEntry("k.a", "2", "QUEUE", "k.a")));
    ValidationResult explained = result.withExplain(explain);
    assertTrue(explained.hasExplain());
    assertEquals(Arrays.asList("root.b", "root.a"),
        new ArrayList<>(explained.getExplain().keySet()));
    assertNotEquals(result, explained);
    assertEquals(explained, ValidationResult.valid().withExplain(explain));
  }

  @Test
  public void testIssueEqualityAndNullability() {
    ValidationIssue a = ValidationIssue.error(null, null, "r", "m");
    ValidationIssue b = new ValidationIssue(null, null, "r",
        ValidationIssue.Severity.ERROR, "m");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, ValidationIssue.warning(null, null, "r", "m"));
    assertEquals("ERROR r queue=root.a key=k: m",
        ValidationIssue.error("root.a", "k", "r", "m").toString());
    assertThrows(NullPointerException.class,
        () -> ValidationIssue.error(null, null, null, "m"));
  }

  @Test
  public void testExplainOnlyWhenRequested() {
    ConfigSnapshot proposed = ConfigSnapshot.of(new Configuration(false));
    CSConfigValidator validator = new CSConfigValidator();
    ValidationResult plain = validator.validate(proposed, ClusterFacts.empty());
    assertFalse(plain.hasExplain());
    ValidationResult explained = validator.validate(proposed,
        ClusterFacts.empty(), Collections.singletonList("root"));
    assertTrue(explained.hasExplain());
    assertFalse(validator.validate(proposed, ClusterFacts.empty(), null)
        .hasExplain());
  }
}
