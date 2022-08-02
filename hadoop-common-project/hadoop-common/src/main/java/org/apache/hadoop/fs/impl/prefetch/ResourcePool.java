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

import java.io.Closeable;

/**
 * Manages a fixed pool of resources.
 *
 * Avoids creating a new resource if a previously created instance is already available.
 */
public abstract class ResourcePool<T> implements Closeable {

  /**
   * Acquires a resource blocking if necessary until one becomes available.
   *
   * @return the acquired resource instance.
   */
  public abstract T acquire();

  /**
   * Acquires a resource blocking if one is immediately available. Otherwise returns null.

   * @return the acquired resource instance (if immediately available) or null.
   */
  public abstract T tryAcquire();

  /**
   * Releases a previously acquired resource.
   *
   * @param item the resource to release.
   */
  public abstract void release(T item);

  @Override
  public void close() {
  }

  /**
   * Derived classes may implement a way to cleanup each item.
   *
   * @param item the resource to close.
   */
  protected void close(T item) {
    // Do nothing in this class. Allow overriding classes to take any cleanup action.
  }

  /**
   * Derived classes must implement a way to create an instance of a resource.
   *
   * @return the created instance.
   */
  protected abstract T createNew();
}
