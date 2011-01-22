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

package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

class UgiInstrumentation implements MetricsSource {

  final MetricsRegistry registry = new MetricsRegistry("ugi").setContext("ugi");
  final MetricMutableStat loginSuccess = registry.newStat("loginSuccess");
  final MetricMutableStat loginFailure = registry.newStat("loginFailure");

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

  //@Override
  void addLoginSuccess(long elapsed) {
    loginSuccess.add(elapsed);
  }

  //@Override
  void addLoginFailure(long elapsed) {
    loginFailure.add(elapsed);
  }

  static UgiInstrumentation create(Configuration conf) {
    return create(conf, DefaultMetricsSystem.INSTANCE);
  }

  static UgiInstrumentation create(Configuration conf, MetricsSystem ms) {
    return ms.register("ugi", "User/group metrics", new UgiInstrumentation());
  }

}
