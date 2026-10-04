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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the validation engine over the configurations of
 * {@link TestQueueConfigResolverParity} and compares it with the outcome of
 * refreshing a scheduler to each of them: a configuration the refresh
 * accepts validates without an ERROR, against the scheduler it was
 * refreshed from and against a scheduler already running it; a
 * configuration the refresh rejects has an ERROR with the root cause message
 * of the refresh failure, and the legacy validate failure is exactly the
 * refresh failure.
 */
public class TestCSConfigValidatorCorpus {
  private static final String P = CapacitySchedulerConfiguration.PREFIX;

  static Stream<Arguments> configurations() throws Exception {
    Map<String, Configuration> all =
        new LinkedHashMap<>(TestQueueConfigResolverParity.corpus());
    all.putAll(TestQueueConfigResolverParity.rejections());
    return all.entrySet().stream()
        .map(e -> Arguments.of(e.getKey(), e.getValue()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("configurations")
  public void testEngineAgreesWithRefresh(String name, Configuration target)
      throws Exception {
    ValidationResult fromBase;
    IOException failure = null;
    try (MockRM rm = TestQueueConfigResolverParity.startRM(base(target),
        target)) {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      rm.registerNode("h1:1234", 100 * 1024, 100);
      Configuration proposed = proposed(cs, target);
      fromBase = validate(cs, proposed);
      try {
        // The validating refresh runs the queue checks without consulting
        // the validation engine, so its outcome does not depend on it
        cs.reinitialize(proposed, rm.getRMContext(), true);
      } catch (IOException e) {
        failure = e;
      }
    }

    if (failure == null) {
      assertNoErrors(name, "refreshed scheduler", fromBase);
      try (MockRM rm = TestQueueConfigResolverParity.startRM(target, target)) {
        CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
        rm.registerNode("h1:1234", 100 * 1024, 100);
        assertNoErrors(name, "running scheduler",
            validate(cs, proposed(cs, target)));
      }
    } else {
      Throwable rootCause = failure;
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      boolean found = false;
      for (ValidationIssue issue : fromBase.getIssues()) {
        found |= issue.isError()
            && issue.getMessage().equals(rootCause.getMessage());
      }
      assertTrue(found, name + ": no ERROR with message "
          + rootCause.getMessage() + " in " + fromBase.getIssues());
      IOException legacy = assertThrows(IOException.class,
          () -> CapacitySchedulerConfigValidator.throwIfInvalid(fromBase));
      assertEquals(failure.toString(), legacy.toString(), name);
    }
  }

  /**
   * The scheduler a refresh starts from: the global settings of the target
   * and a stopped default queue, which any target can remove or convert.
   */
  private static Configuration base(Configuration target) {
    Configuration base = new Configuration(false);
    for (Map.Entry<String, String> e : target) {
      if (!e.getKey().startsWith(P)) {
        base.set(e.getKey(), e.getValue());
      }
    }
    for (String key : new String[] {P + "legacy-queue-mode.enabled",
        CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS}) {
      if (target.getRaw(key) != null) {
        base.set(key, target.getRaw(key));
      }
    }
    base.set(P + "root.queues", "default");
    base.set(P + "root.default.capacity", "100");
    base.set(P + "root.default.state", "STOPPED");
    return base;
  }

  private static Configuration proposed(CapacityScheduler cs,
      Configuration target) {
    Configuration proposed = new Configuration(cs.getConf());
    for (Map.Entry<String, String> e : target) {
      proposed.set(e.getKey(), e.getValue());
    }
    return proposed;
  }

  private static ValidationResult validate(CapacityScheduler cs,
      Configuration proposed) {
    return new CSConfigValidator().validate(ConfigSnapshot.of(proposed),
        ClusterFacts.capture(cs));
  }

  private static void assertNoErrors(String name, String against,
      ValidationResult result) {
    List<ValidationIssue> errors = new ArrayList<>();
    for (ValidationIssue issue : result.getIssues()) {
      if (issue.isError()) {
        errors.add(issue);
      }
    }
    assertTrue(errors.isEmpty(), name + " against the " + against + ": "
        + errors);
  }
}
