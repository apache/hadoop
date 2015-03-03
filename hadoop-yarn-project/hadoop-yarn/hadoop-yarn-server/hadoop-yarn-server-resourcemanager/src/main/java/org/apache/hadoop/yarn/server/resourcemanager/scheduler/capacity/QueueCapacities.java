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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

import com.google.common.collect.Sets;

public class QueueCapacities {
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;
  private static final float LABEL_DOESNT_EXIST_CAP = 0f;
  private Map<String, Capacities> capacitiesMap;
  private ReadLock readLock;
  private WriteLock writeLock;
  private final boolean isRoot;

  public QueueCapacities(boolean isRoot) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    capacitiesMap = new HashMap<String, Capacities>();
    this.isRoot = isRoot;
  }
  
  // Usage enum here to make implement cleaner
  private enum CapacityType {
    USED_CAP(0), ABS_USED_CAP(1), MAX_CAP(2), ABS_MAX_CAP(3), CAP(4), ABS_CAP(5);

    private int idx;

    private CapacityType(int idx) {
      this.idx = idx;
    }
  }

  private static class Capacities {
    private float[] capacitiesArr;
    
    public Capacities() {
      capacitiesArr = new float[CapacityType.values().length];
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{used=" + capacitiesArr[0] + "%, ");
      sb.append("abs_used=" + capacitiesArr[1] + "%, ");
      sb.append("max_cap=" + capacitiesArr[2] + "%, ");
      sb.append("abs_max_cap=" + capacitiesArr[3] + "%, ");
      sb.append("cap=" + capacitiesArr[4] + "%, ");
      sb.append("abs_cap=" + capacitiesArr[5] + "%}");
      return sb.toString();
    }
  }
  
  private float _get(String label, CapacityType type) {
    try {
      readLock.lock();
      Capacities cap = capacitiesMap.get(label);
      if (null == cap) {
        return LABEL_DOESNT_EXIST_CAP;
      }
      return cap.capacitiesArr[type.idx];
    } finally {
      readLock.unlock();
    }
  }
  
  private void _set(String label, CapacityType type, float value) {
    try {
      writeLock.lock();
      Capacities cap = capacitiesMap.get(label);
      if (null == cap) {
        cap = new Capacities();
        capacitiesMap.put(label, cap);
      }
      cap.capacitiesArr[type.idx] = value;
    } finally {
      writeLock.unlock();
    }
  }

  /* Used Capacity Getter and Setter */
  public float getUsedCapacity() {
    return _get(NL, CapacityType.USED_CAP);
  }

  public float getUsedCapacity(String label) {
    return _get(label, CapacityType.USED_CAP);
  }

  public void setUsedCapacity(float value) {
    _set(NL, CapacityType.USED_CAP, value);
  }

  public void setUsedCapacity(String label, float value) {
    _set(label, CapacityType.USED_CAP, value);
  }

  /* Absolute Used Capacity Getter and Setter */
  public float getAbsoluteUsedCapacity() {
    return _get(NL, CapacityType.ABS_USED_CAP);
  }

  public float getAbsoluteUsedCapacity(String label) {
    return _get(label, CapacityType.ABS_USED_CAP);
  }

  public void setAbsoluteUsedCapacity(float value) {
    _set(NL, CapacityType.ABS_USED_CAP, value);
  }

  public void setAbsoluteUsedCapacity(String label, float value) {
    _set(label, CapacityType.ABS_USED_CAP, value);
  }

  /* Capacity Getter and Setter */
  public float getCapacity() {
    return _get(NL, CapacityType.CAP);
  }

  public float getCapacity(String label) {
    if (StringUtils.equals(label, RMNodeLabelsManager.NO_LABEL) && isRoot) {
      return 1f;
    }
    
    return _get(label, CapacityType.CAP);
  }

  public void setCapacity(float value) {
    _set(NL, CapacityType.CAP, value);
  }

  public void setCapacity(String label, float value) {
    _set(label, CapacityType.CAP, value);
  }

  /* Absolute Capacity Getter and Setter */
  public float getAbsoluteCapacity() {
    return _get(NL, CapacityType.ABS_CAP);
  }

  public float getAbsoluteCapacity(String label) {
    if (StringUtils.equals(label, RMNodeLabelsManager.NO_LABEL) && isRoot) {
      return 1f;
    }
    return _get(label, CapacityType.ABS_CAP);
  }

  public void setAbsoluteCapacity(float value) {
    _set(NL, CapacityType.ABS_CAP, value);
  }

  public void setAbsoluteCapacity(String label, float value) {
    _set(label, CapacityType.ABS_CAP, value);
  }

  /* Maximum Capacity Getter and Setter */
  public float getMaximumCapacity() {
    return _get(NL, CapacityType.MAX_CAP);
  }

  public float getMaximumCapacity(String label) {
    return _get(label, CapacityType.MAX_CAP);
  }

  public void setMaximumCapacity(float value) {
    _set(NL, CapacityType.MAX_CAP, value);
  }

  public void setMaximumCapacity(String label, float value) {
    _set(label, CapacityType.MAX_CAP, value);
  }

  /* Absolute Maximum Capacity Getter and Setter */
  public float getAbsoluteMaximumCapacity() {
    return _get(NL, CapacityType.ABS_MAX_CAP);
  }

  public float getAbsoluteMaximumCapacity(String label) {
    return _get(label, CapacityType.ABS_MAX_CAP);
  }

  public void setAbsoluteMaximumCapacity(float value) {
    _set(NL, CapacityType.ABS_MAX_CAP, value);
  }

  public void setAbsoluteMaximumCapacity(String label, float value) {
    _set(label, CapacityType.ABS_MAX_CAP, value);
  }
  
  /**
   * Clear configurable fields, like
   * (absolute)capacity/(absolute)maximum-capacity, this will be used by queue
   * reinitialize, when we reinitialize a queue, we will first clear all
   * configurable fields, and load new values
   */
  public void clearConfigurableFields() {
    try {
      writeLock.lock();
      for (String label : capacitiesMap.keySet()) {
        _set(label, CapacityType.CAP, 0);
        _set(label, CapacityType.MAX_CAP, 0);
        _set(label, CapacityType.ABS_CAP, 0);
        _set(label, CapacityType.ABS_MAX_CAP, 0);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public Set<String> getExistingNodeLabels() {
    try {
      readLock.lock();
      return new HashSet<String>(capacitiesMap.keySet());
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public String toString() {
    try {
      readLock.lock();
      return this.capacitiesMap.toString();
    } finally {
      readLock.unlock();
    }
  }
}
