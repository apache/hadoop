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

package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Class that encapsulates the cache resource. The instances are not thread
 * safe. Any operation that uses the resource must use thread-safe mechanisms to
 * ensure safe access with the only exception of the filename.
 */
@Private
@Evolving
class SharedCacheResource {
  private long accessTime;
  private final Set<SharedCacheResourceReference> refs;
  private final String fileName;

  SharedCacheResource(String fileName) {
    this.accessTime = System.currentTimeMillis();
    this.refs = new HashSet<SharedCacheResourceReference>();
    this.fileName = fileName;
  }

  long getAccessTime() {
    return accessTime;
  }

  void updateAccessTime() {
    accessTime = System.currentTimeMillis();
  }

  String getFileName() {
    return this.fileName;
  }

  Set<SharedCacheResourceReference> getResourceReferences() {
    return this.refs;
  }

  boolean addReference(SharedCacheResourceReference ref) {
    return this.refs.add(ref);
  }
}