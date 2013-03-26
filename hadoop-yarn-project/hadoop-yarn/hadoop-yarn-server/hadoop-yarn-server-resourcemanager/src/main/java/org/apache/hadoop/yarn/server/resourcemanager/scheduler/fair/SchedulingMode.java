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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.modes.FairSchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.modes.FifoSchedulingMode;

@Public
@Unstable
public abstract class SchedulingMode {
  private static final ConcurrentHashMap<Class<? extends SchedulingMode>, SchedulingMode> instances =
      new ConcurrentHashMap<Class<? extends SchedulingMode>, SchedulingMode>();

  private static SchedulingMode DEFAULT_MODE =
      getInstance(FairSchedulingMode.class);
  
  public static SchedulingMode getDefault() {
    return DEFAULT_MODE;
  }

  public static void setDefault(String className)
      throws AllocationConfigurationException {
    DEFAULT_MODE = parse(className);
  }

  /**
   * Returns a {@link SchedulingMode} instance corresponding to the passed clazz
   */
  public static SchedulingMode getInstance(Class<? extends SchedulingMode> clazz) {
    SchedulingMode mode = instances.get(clazz);
    if (mode == null) {
      mode = ReflectionUtils.newInstance(clazz, null);
      instances.put(clazz, mode);
    }
    return mode;
  }

  /**
   * Returns {@link SchedulingMode} instance corresponding to the
   * {@link SchedulingMode} passed as a string. The mode can be "fair" for
   * FairSchedulingMode of "fifo" for FifoSchedulingMode. For custom
   * {@link SchedulingMode}s in the RM classpath, the mode should be canonical
   * class name of the {@link SchedulingMode}.
   * 
   * @param mode canonical class name or "fair" or "fifo"
   * @throws AllocationConfigurationException
   */
  @SuppressWarnings("unchecked")
  public static SchedulingMode parse(String mode)
      throws AllocationConfigurationException {
    @SuppressWarnings("rawtypes")
    Class clazz;
    String text = mode.toLowerCase();
    if (text.equals("fair")) {
      clazz = FairSchedulingMode.class;
    } else if (text.equals("fifo")) {
      clazz = FifoSchedulingMode.class;
    } else {
      try {
        clazz = Class.forName(mode);
      } catch (ClassNotFoundException cnfe) {
        throw new AllocationConfigurationException(mode
            + " SchedulingMode class not found!");
      }
    }
    if (!SchedulingMode.class.isAssignableFrom(clazz)) {
      throw new AllocationConfigurationException(mode
          + " does not extend SchedulingMode");
    }
    return getInstance(clazz);
  }

  /**
   * @return returns the name of SchedulingMode
   */
  public abstract String getName();

  /**
   * The comparator returned by this method is to be used for sorting the
   * {@link Schedulable}s in that queue.
   * 
   * @return the comparator to sort by
   */
  public abstract Comparator<Schedulable> getComparator();

  /**
   * Computes and updates the shares of {@link Schedulable}s as per the
   * SchedulingMode, to be used later at schedule time.
   * 
   * @param schedulables {@link Schedulable}s whose shares are to be updated
   * @param totalResources Total {@link Resource}s in the cluster
   */
  public abstract void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources);
}
