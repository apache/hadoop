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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Represents a node in the cluster from the NodeQueueLoadMonitor's perspective
 */
public class ClusterNode {
  /**
   * Properties class used to initialize/change fields in ClusterNode.
   */
  public static final class Properties {
    private int queueLength = 0;
    private int queueWaitTime = -1;
    private long timestamp;
    private int queueCapacity = 0;
    private boolean queueCapacityIsSet = false;
    private final HashSet<String> labels;
    private Resource capability = null;
    private Resource allocatedResource = null;

    public static Properties newInstance() {
      return new Properties();
    }

    Properties setQueueLength(int qLength) {
      this.queueLength = qLength;
      return this;
    }

    Properties setQueueWaitTime(int wTime) {
      this.queueWaitTime = wTime;
      return this;
    }

    Properties updateTimestamp() {
      this.timestamp = System.currentTimeMillis();
      return this;
    }

    Properties setQueueCapacity(int capacity) {
      this.queueCapacity = capacity;
      this.queueCapacityIsSet = true;
      return this;
    }

    Properties setNodeLabels(Collection<String> labelsToAdd) {
      labels.clear();
      labels.addAll(labelsToAdd);
      return this;
    }

    Properties setCapability(Resource nodeCapability) {
      this.capability = nodeCapability;
      return this;
    }

    Properties setAllocatedResource(Resource allocResource) {
      this.allocatedResource = allocResource;
      return this;
    }

    private Properties() {
      labels = new HashSet<>();
    }
  }

  private int queueLength = 0;
  private int queueWaitTime = -1;
  private long timestamp;
  final NodeId nodeId;
  private int queueCapacity = 0;
  private final HashSet<String> labels;
  private Resource capability = Resources.none();
  private Resource allocatedResource = Resources.none();
  private final ReentrantReadWriteLock.WriteLock writeLock;
  private final ReentrantReadWriteLock.ReadLock readLock;

  public ClusterNode(NodeId nodeId) {
    this.nodeId = nodeId;
    this.labels = new HashSet<>();
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.writeLock = lock.writeLock();
    this.readLock = lock.readLock();
    this.timestamp = System.currentTimeMillis();
  }

  public ClusterNode setProperties(final Properties properties) {
    writeLock.lock();
    try {
      if (properties.capability == null) {
        this.capability = Resources.none();
      } else {
        this.capability = properties.capability;
      }

      if (properties.allocatedResource == null) {
        this.allocatedResource = Resources.none();
      } else {
        this.allocatedResource = properties.allocatedResource;
      }

      this.queueLength = properties.queueLength;
      this.queueWaitTime = properties.queueWaitTime;
      this.timestamp = properties.timestamp;
      if (properties.queueCapacityIsSet) {
        // queue capacity is only set on node add, not on node updates
        this.queueCapacity = properties.queueCapacity;
      }
      this.labels.clear();
      this.labels.addAll(properties.labels);
      return this;
    } finally {
      writeLock.unlock();
    }
  }

  public Resource getAllocatedResource() {
    readLock.lock();
    try {
      return this.allocatedResource;
    } finally {
      readLock.unlock();
    }
  }

  public Resource getAvailableResource() {
    readLock.lock();
    try {
      return Resources.subtractNonNegative(capability, allocatedResource);
    } finally {
      readLock.unlock();
    }
  }

  public Resource getCapability() {
    readLock.lock();
    try {
      return this.capability;
    } finally {
      readLock.unlock();
    }
  }

  public boolean hasLabel(String label) {
    readLock.lock();
    try {
      return this.labels.contains(label);
    } finally {
      readLock.unlock();
    }
  }

  public long getTimestamp() {
    readLock.lock();
    try {
      return this.timestamp;
    } finally {
      readLock.unlock();
    }
  }

  public int getQueueLength() {
    readLock.lock();
    try {
      return this.queueLength;
    } finally {
      readLock.unlock();
    }
  }

  public int getQueueWaitTime() {
    readLock.lock();
    try {
      return this.queueWaitTime;
    } finally {
      readLock.unlock();
    }
  }

  public int getQueueCapacity() {
    readLock.lock();
    try {
      return this.queueCapacity;
    } finally {
      readLock.unlock();
    }
  }

  public boolean compareAndIncrementAllocation(
      final int incrementQLen,
      final ResourceCalculator resourceCalculator,
      final Resource requested) {
    writeLock.lock();
    try {
      final Resource currAvailable = Resources.subtractNonNegative(
          capability, allocatedResource);
      if (resourceCalculator.fitsIn(requested, currAvailable)) {
        allocatedResource = Resources.add(allocatedResource, requested);
        return true;
      }

      if (!resourceCalculator.fitsIn(requested, capability)) {
        // If does not fit at all, do not allocate
        return false;
      }

      return compareAndIncrementAllocation(incrementQLen);
    } finally {
      writeLock.unlock();
    }
  }

  public boolean compareAndIncrementAllocation(final int incrementQLen) {
    writeLock.lock();
    try {
      final int added = queueLength + incrementQLen;
      if (added <= queueCapacity) {
        queueLength = added;
        return true;
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isQueueFull() {
    readLock.lock();
    try {
      return this.queueCapacity > 0 &&
          this.queueLength >= this.queueCapacity;
    } finally {
      readLock.unlock();
    }
  }
}
