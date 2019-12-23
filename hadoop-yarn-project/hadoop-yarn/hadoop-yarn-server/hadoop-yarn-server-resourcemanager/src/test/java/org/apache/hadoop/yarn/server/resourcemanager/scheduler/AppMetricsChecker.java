/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import com.google.common.collect.Maps;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_COMPLETED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_FAILED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_KILLED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_PENDING;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_RUNNING;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_SUBMITTED;

final class AppMetricsChecker {
  private final static Logger LOG =
      LoggerFactory.getLogger(AppMetricsChecker.class);

  private static final AppMetricsChecker INITIAL_CHECKER =
      new AppMetricsChecker()
        .counter(APPS_SUBMITTED, 0)
        .gaugeInt(APPS_PENDING, 0)
        .gaugeInt(APPS_RUNNING, 0)
        .counter(APPS_COMPLETED, 0)
        .counter(APPS_FAILED, 0)
        .counter(APPS_KILLED, 0);

  enum AppMetricsKey {
    APPS_SUBMITTED("AppsSubmitted"),
    APPS_PENDING("AppsPending"),
    APPS_RUNNING("AppsRunning"),
    APPS_COMPLETED("AppsCompleted"),
    APPS_FAILED("AppsFailed"),
    APPS_KILLED("AppsKilled");

    private String value;

    AppMetricsKey(String value) {
      this.value = value;
    }
  }
  private final Map<AppMetricsKey, Integer> gaugesInt;
  private final Map<AppMetricsKey, Integer> counters;

  private AppMetricsChecker() {
    this.gaugesInt = Maps.newHashMap();
    this.counters = Maps.newHashMap();
  }

  private AppMetricsChecker(AppMetricsChecker checker) {
    this.gaugesInt = Maps.newHashMap(checker.gaugesInt);
    this.counters = Maps.newHashMap(checker.counters);
  }

  public static AppMetricsChecker createFromChecker(AppMetricsChecker checker) {
    return new AppMetricsChecker(checker);
  }

  public static AppMetricsChecker create() {
    return new AppMetricsChecker(INITIAL_CHECKER);
  }

  AppMetricsChecker gaugeInt(AppMetricsKey key, int value) {
    gaugesInt.put(key, value);
    return this;
  }

  AppMetricsChecker counter(AppMetricsKey key, int value) {
    counters.put(key, value);
    return this;
  }

  AppMetricsChecker checkAgainst(MetricsSource source, boolean all) {
    if (source == null) {
      throw new IllegalStateException(
          "MetricsSource should not be null!");
    }
    MetricsRecordBuilder recordBuilder = getMetrics(source, all);
    logAssertingMessage(source);

    for (Map.Entry<AppMetricsKey, Integer> gauge : gaugesInt.entrySet()) {
      assertGauge(gauge.getKey().value, gauge.getValue(), recordBuilder);
    }

    for (Map.Entry<AppMetricsKey, Integer> counter : counters.entrySet()) {
      assertCounter(counter.getKey().value, counter.getValue(), recordBuilder);
    }
    return this;
  }

  private void logAssertingMessage(MetricsSource source) {
    String queueName = ((QueueMetrics) source).queueName;
    Map<String, QueueMetrics> users = ((QueueMetrics) source).users;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Asserting App metrics.. QueueName: " + queueName + ", users: "
          + (users != null && !users.isEmpty() ? users : ""));
    }
  }
}
