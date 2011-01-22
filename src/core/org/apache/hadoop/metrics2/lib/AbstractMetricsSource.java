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

package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;

/**
 * A convenient base class for writing metrics sources
 */
public abstract class AbstractMetricsSource implements MetricsSource {

  protected final MetricsRegistry registry;

  /**
   * Construct the source with name and a mutable metrics factory
   * @param name  of the default record
   * @param mf  the factory to create mutable metrics
   */
  public AbstractMetricsSource(String name, MetricMutableFactory mf) {
    registry = new MetricsRegistry(name, mf);
  }

  /**
   * Construct the source with a name with a default factory
   * @param name  of the default record
   */
  public AbstractMetricsSource(String name) {
    this(name, new MetricMutableFactory());
  }

  /**
   * @return  the registry for mutable metrics
   */
  public MetricsRegistry registry() {
    return registry;
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

}