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

import java.lang.reflect.Method;
import java.util.Set;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * Helper class to manage a group of mutable rate metrics
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableRates extends MutableMetric {
  static final Log LOG = LogFactory.getLog(MutableRates.class);
  private final MetricsRegistry registry;
  private final Set<Class<?>> protocolCache = Sets.newHashSet();

  MutableRates(MetricsRegistry registry) {
    this.registry = checkNotNull(registry, "metrics registry");
  }

  /**
   * Initialize the registry with all the methods in a protocol
   * so they all show up in the first snapshot.
   * Convenient for JMX implementations.
   * @param protocol the protocol class
   */
  public void init(Class<?> protocol) {
    if (protocolCache.contains(protocol)) return;
    protocolCache.add(protocol);
    for (Method method : protocol.getDeclaredMethods()) {
      String name = method.getName();
      LOG.debug(name);
      try { registry.newRate(name, name, false, true); }
      catch (Exception e) {
        LOG.error("Error creating rate metrics for "+ method.getName(), e);
      }
    }
  }

  /**
   * Add a rate sample for a rate metric
   * @param name of the rate metric
   * @param elapsed time
   */
  public void add(String name, long elapsed) {
    registry.add(name, elapsed);
  }

  @Override
  public void snapshot(MetricsRecordBuilder rb, boolean all) {
    registry.snapshot(rb, all);
  }
}
