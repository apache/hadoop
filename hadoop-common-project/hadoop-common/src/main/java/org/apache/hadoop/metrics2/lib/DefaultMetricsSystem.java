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

import java.util.concurrent.atomic.AtomicReference;
import javax.management.ObjectName;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

/**
 * The default metrics system singleton
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum DefaultMetricsSystem {
  INSTANCE; // the singleton

  private AtomicReference<MetricsSystem> impl =
      new AtomicReference<MetricsSystem>(new MetricsSystemImpl());
  volatile boolean miniClusterMode = false;
  transient final UniqueNames mBeanNames = new UniqueNames();
  transient final UniqueNames sourceNames = new UniqueNames();

  /**
   * Convenience method to initialize the metrics system
   * @param prefix  for the metrics system configuration
   * @return the metrics system instance
   */
  public static MetricsSystem initialize(String prefix) {
    return INSTANCE.init(prefix);
  }

  MetricsSystem init(String prefix) {
    return impl.get().init(prefix);
  }

  /**
   * @return the metrics system object
   */
  public static MetricsSystem instance() {
    return INSTANCE.getImpl();
  }

  /**
   * Shutdown the metrics system
   */
  public static void shutdown() {
    INSTANCE.shutdownInstance();
  }

  void shutdownInstance() {
    boolean last = impl.get().shutdown();
    if (last) synchronized(this) {
      mBeanNames.map.clear();
      sourceNames.map.clear();
    }
  }

  @InterfaceAudience.Private
  public static MetricsSystem setInstance(MetricsSystem ms) {
    return INSTANCE.setImpl(ms);
  }

  MetricsSystem setImpl(MetricsSystem ms) {
    return impl.getAndSet(ms);
  }

  MetricsSystem getImpl() { return impl.get(); }

  @InterfaceAudience.Private
  public static void setMiniClusterMode(boolean choice) {
    INSTANCE.miniClusterMode = choice;
  }

  @InterfaceAudience.Private
  public static boolean inMiniClusterMode() {
    return INSTANCE.miniClusterMode;
  }

  @InterfaceAudience.Private
  public static ObjectName newMBeanName(String name) {
    return INSTANCE.newObjectName(name);
  }

  @InterfaceAudience.Private
  public static void removeMBeanName(ObjectName name) {
    INSTANCE.removeObjectName(name.toString());
  }

  @InterfaceAudience.Private
  public static String sourceName(String name, boolean dupOK) {
    return INSTANCE.newSourceName(name, dupOK);
  }

  synchronized ObjectName newObjectName(String name) {
    try {
      if (mBeanNames.map.containsKey(name) && !miniClusterMode) {
        throw new MetricsException(name +" already exists!");
      }
      return new ObjectName(mBeanNames.uniqueName(name));
    } catch (Exception e) {
      throw new MetricsException(e);
    }
  }

  synchronized void removeObjectName(String name) {
    mBeanNames.map.remove(name);
  }

  synchronized String newSourceName(String name, boolean dupOK) {
    if (sourceNames.map.containsKey(name)) {
      if (dupOK) {
        return name;
      } else if (!miniClusterMode) {
        throw new MetricsException("Metrics source "+ name +" already exists!");
      }
    }
    return sourceNames.uniqueName(name);
  }
}
