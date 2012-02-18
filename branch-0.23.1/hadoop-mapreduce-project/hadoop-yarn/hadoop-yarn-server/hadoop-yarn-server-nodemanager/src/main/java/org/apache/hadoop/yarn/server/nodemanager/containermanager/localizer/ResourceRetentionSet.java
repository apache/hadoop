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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.yarn.server.nodemanager.DeletionService;

public class ResourceRetentionSet {

  private long delSize;
  private long currentSize;
  private final long targetSize;
  private final DeletionService delService;
  private final SortedMap<LocalizedResource,LocalResourcesTracker> retain;

  ResourceRetentionSet(DeletionService delService, long targetSize) {
    this(delService, targetSize, new LRUComparator());
  }

  ResourceRetentionSet(DeletionService delService, long targetSize,
      Comparator<? super LocalizedResource> cmp) {
    this(delService, targetSize,
        new TreeMap<LocalizedResource,LocalResourcesTracker>(cmp));
  }

  ResourceRetentionSet(DeletionService delService, long targetSize,
      SortedMap<LocalizedResource,LocalResourcesTracker> retain) {
    this.retain = retain;
    this.delService = delService;
    this.targetSize = targetSize;
  }

  public void addResources(LocalResourcesTracker newTracker) {
    for (LocalizedResource resource : newTracker) {
      currentSize += resource.getSize();
      if (resource.getRefCount() > 0) {
        // always retain resources in use
        continue;
      }
      retain.put(resource, newTracker);
    }
    for (Iterator<Map.Entry<LocalizedResource,LocalResourcesTracker>> i =
           retain.entrySet().iterator();
         currentSize - delSize > targetSize && i.hasNext();) {
      Map.Entry<LocalizedResource,LocalResourcesTracker> rsrc = i.next();
      LocalizedResource resource = rsrc.getKey();
      LocalResourcesTracker tracker = rsrc.getValue();
      if (tracker.remove(resource, delService)) {
        delSize += resource.getSize();
        i.remove();
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Cache: ").append(currentSize).append(", ");
    sb.append("Deleted: ").append(delSize);
    return sb.toString();
  }

  static class LRUComparator implements Comparator<LocalizedResource> {
    public int compare(LocalizedResource r1, LocalizedResource r2) {
      long ret = r1.getTimestamp() - r2.getTimestamp();
      if (0 == ret) {
        return System.identityHashCode(r1) - System.identityHashCode(r2);
      }
      return ret > 0 ? 1 : -1;
    }
    public boolean equals(Object other) {
      return this == other;
    }
  }
}
