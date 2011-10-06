/**
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.io.hfile.slab;

/**
 * Interface for objects that want to know when an eviction occurs.
 * */
interface SlabItemEvictionWatcher {

  /**
   * This is called as a callback by the EvictionListener in each of the
   * SingleSizeSlabCaches.
   *
   * @param key the key of the item being evicted
   * @param notifier the object notifying the SlabCache of the eviction.
   * @param boolean callAssignedCache whether we should call the cache which the
   *        key was originally assigned to.
   */
  void onEviction(String key, Object notifier);

}
