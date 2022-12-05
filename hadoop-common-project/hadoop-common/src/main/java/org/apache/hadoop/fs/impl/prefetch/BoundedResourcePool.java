/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.fs.impl.prefetch.Validate.checkNotNull;

/**
 * Manages a fixed pool of resources.
 *
 * Avoids creating a new resource if a previously created instance is already available.
 */
public abstract class BoundedResourcePool<T> extends ResourcePool<T> {
  /**
   * The size of this pool. Fixed at creation time.
   */
  private final int size;

  /**
   * Items currently available in the pool.
   */
  private ArrayBlockingQueue<T> items;

  /**
   * Items that have been created so far (regardless of whether they are currently available).
   */
  private Set<T> createdItems;

  /**
   * Constructs a resource pool of the given size.
   *
   * @param size the size of this pool. Cannot be changed post creation.
   *
   * @throws IllegalArgumentException if size is zero or negative.
   */
  public BoundedResourcePool(int size) {
    Validate.checkPositiveInteger(size, "size");

    this.size = size;
    this.items = new ArrayBlockingQueue<>(size);

    // The created items are identified based on their object reference.
    this.createdItems = Collections.newSetFromMap(new IdentityHashMap<T, Boolean>());
  }

  /**
   * Acquires a resource blocking if necessary until one becomes available.
   */
  @Override
  public T acquire() {
    return this.acquireHelper(true);
  }

  /**
   * Acquires a resource blocking if one is immediately available. Otherwise returns null.
   */
  @Override
  public T tryAcquire() {
    return this.acquireHelper(false);
  }

  /**
   * Releases a previously acquired resource.
   *
   * @throws IllegalArgumentException if item is null.
   */
  @Override
  public void release(T item) {
    checkNotNull(item, "item");

    synchronized (createdItems) {
      if (!createdItems.contains(item)) {
        throw new IllegalArgumentException("This item is not a part of this pool");
      }
    }

    // Return if this item was released earlier.
    // We cannot use items.contains() because that check is not based on reference equality.
    for (T entry : items) {
      if (entry == item) {
        return;
      }
    }

    try {
      items.put(item);
    } catch (InterruptedException e) {
      throw new IllegalStateException("release() should never block", e);
    }
  }

  @Override
  public synchronized void close() {
    for (T item : createdItems) {
      close(item);
    }

    items.clear();
    items = null;

    createdItems.clear();
    createdItems = null;
  }

  /**
   * Derived classes may implement a way to cleanup each item.
   */
  @Override
  protected synchronized void close(T item) {
    // Do nothing in this class. Allow overriding classes to take any cleanup action.
  }

  /**
   * Number of items created so far. Mostly for testing purposes.
   * @return the count.
   */
  public int numCreated() {
    synchronized (createdItems) {
      return createdItems.size();
    }
  }

  /**
   * Number of items available to be acquired. Mostly for testing purposes.
   * @return the number available.
   */
  public synchronized int numAvailable() {
    return (size - numCreated()) + items.size();
  }

  // For debugging purposes.
  @Override
  public synchronized String toString() {
    return String.format(
        "size = %d, #created = %d, #in-queue = %d, #available = %d",
        size, numCreated(), items.size(), numAvailable());
  }

  /**
   * Derived classes must implement a way to create an instance of a resource.
   */
  protected abstract T createNew();

  private T acquireHelper(boolean canBlock) {

    // Prefer reusing an item if one is available.
    // That avoids unnecessarily creating new instances.
    T result = items.poll();
    if (result != null) {
      return result;
    }

    synchronized (createdItems) {
      // Create a new instance if allowed by the capacity of this pool.
      if (createdItems.size() < size) {
        T item = createNew();
        createdItems.add(item);
        return item;
      }
    }

    if (canBlock) {
      try {
        // Block for an instance to be available.
        return items.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    } else {
      return null;
    }
  }
}
