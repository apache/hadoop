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

import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

/**
 * The default metrics system singleton
 */
public enum DefaultMetricsSystem implements MetricsSystem {

  /**
   * The singleton instance
   */
  INSTANCE;

  private static final int VERSION = 2;
  private final MetricsSystemImpl impl = new MetricsSystemImpl();

  private MetricsSystem init(String prefix) {
    impl.init(prefix);
    return impl;
  }

  /**
   * Common static convenience method to initialize the metrics system
   * @param prefix  for configuration
   * @return the metrics system instance
   */
  public static MetricsSystem initialize(String prefix) {
    return INSTANCE.init(prefix);
  }

  public <T extends MetricsSource> T
  register(String name, String desc, T source) {
    return impl.register(name, desc, source);
  }

  /**
   * Common static method to register a source
   * @param <T>   type of the source
   * @param name  of the source
   * @param desc  description
   * @param source  the source object to register
   * @return the source object
   */
  public static <T extends MetricsSource> T
  registerSource(String name, String desc, T source) {
    return INSTANCE.register(name, desc, source);
  }

  public <T extends MetricsSink> T register(String name, String desc, T sink) {
    return impl.register(name, desc, sink);
  }

  public void register(Callback callback) {
    impl.register(callback);
  }

  public void start() {
    impl.start();
  }

  public void stop() {
    impl.stop();
  }

  public void refreshMBeans() {
    impl.refreshMBeans();
  }

  public String currentConfig() {
    return impl.currentConfig();
  }

  public void shutdown() {
    impl.shutdown();
  }

}
