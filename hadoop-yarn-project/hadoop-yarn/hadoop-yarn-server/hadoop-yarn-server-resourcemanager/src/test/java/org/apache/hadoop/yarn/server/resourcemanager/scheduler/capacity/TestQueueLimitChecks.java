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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueLimitChecks.AppLifetimeInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueLimitChecks.UserWeightsInput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestQueueLimitChecks {

  @Test
  public void testUserWeightsPass() {
    Map<String, Float> weights = new HashMap<>();
    weights.put("alice", 0.0f);
    weights.put("bob", 2.0f);
    assertNull(QueueLimitChecks.checkUserWeights(
        new UserWeightsInput("root.a", 50.0f, weights)));
    assertNull(QueueLimitChecks.checkUserWeights(
        new UserWeightsInput("root.a", 50.0f, Collections.emptyMap())));
  }

  @Test
  public void testUserWeightAboveLimit() {
    assertEquals("Weight (3.0) for user \"alice\" must be between 0 and"
            + " 100 / 50.0 (= 2.0, the number of concurrent active users in"
            + " root.a)",
        QueueLimitChecks.checkUserWeights(new UserWeightsInput("root.a", 50.0f,
            Collections.singletonMap("alice", 3.0f))));
  }

  @Test
  public void testNegativeUserWeight() {
    assertEquals("Weight (-1.0) for user \"bob\" must be between 0 and"
            + " 100 / 25.0 (= 4.0, the number of concurrent active users in"
            + " root.b)",
        QueueLimitChecks.checkUserWeights(new UserWeightsInput("root.b", 25.0f,
            Collections.singletonMap("bob", -1.0f))));
  }

  @Test
  public void testUserLimitAboveHundredIsCapped() {
    assertNull(QueueLimitChecks.checkUserWeights(new UserWeightsInput("root.a",
        200.0f, Collections.singletonMap("alice", 1.0f))));
    assertEquals("Weight (1.5) for user \"alice\" must be between 0 and"
            + " 100 / 100.0 (= 1.0, the number of concurrent active users in"
            + " root.a)",
        QueueLimitChecks.checkUserWeights(new UserWeightsInput("root.a",
            200.0f, Collections.singletonMap("alice", 1.5f))));
  }

  @Test
  public void testFirstInvalidUserInIterationOrderIsReported() {
    Map<String, Float> weights = new LinkedHashMap<>();
    weights.put("ok", 1.0f);
    weights.put("second", 5.0f);
    weights.put("third", 6.0f);
    assertEquals("Weight (5.0) for user \"second\" must be between 0 and"
            + " 100 / 50.0 (= 2.0, the number of concurrent active users in"
            + " root.a)",
        QueueLimitChecks.checkUserWeights(
            new UserWeightsInput("root.a", 50.0f, weights)));
  }

  @Test
  public void testUserWeightsValidateForLeafQueueThrowsIOException() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    QueuePath path = new QueuePath("root.a");
    conf.set(QueuePrefixes.getQueuePrefix(path) + "user-settings.alice.weight",
        "3");
    UserWeights weights = UserWeights.createByConfig(conf,
        conf.getConfigurationProperties(), path);
    IOException e = assertThrows(IOException.class,
        () -> weights.validateForLeafQueue(50.0f, "root.a"));
    assertEquals("Weight (3.0) for user \"alice\" must be between 0 and"
        + " 100 / 50.0 (= 2.0, the number of concurrent active users in"
        + " root.a)", e.getMessage());
  }

  @Test
  public void testDefaultLifetimePass() {
    assertNull(QueueLimitChecks.checkDefaultAppLifetime(
        new AppLifetimeInput(100, 100)));
    assertNull(QueueLimitChecks.checkDefaultAppLifetime(
        new AppLifetimeInput(100, 50)));
    assertNull(QueueLimitChecks.checkDefaultAppLifetime(
        new AppLifetimeInput(100, -1)));
  }

  @Test
  public void testDefaultLifetimeWithUnlimitedMaximum() {
    // -1 (not set) and 0 (explicitly unlimited) never limit the default
    assertNull(QueueLimitChecks.checkDefaultAppLifetime(
        new AppLifetimeInput(-1, 500)));
    assertNull(QueueLimitChecks.checkDefaultAppLifetime(
        new AppLifetimeInput(0, 500)));
  }

  @Test
  public void testDefaultLifetimeAboveMaximum() {
    assertEquals("Default lifetime 200 can't exceed maximum lifetime 100",
        QueueLimitChecks.checkDefaultAppLifetime(
            new AppLifetimeInput(100, 200)));
  }
}
