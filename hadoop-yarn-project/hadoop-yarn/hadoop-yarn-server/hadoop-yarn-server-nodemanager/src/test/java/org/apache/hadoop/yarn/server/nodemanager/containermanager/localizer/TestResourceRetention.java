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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.junit.Test;

import static org.junit.Assert.*;

import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;

public class TestResourceRetention {

  @Test
  public void testRsrcUnused() {
    DeletionService delService = mock(DeletionService.class);
    long TARGET_MB = 10 << 20;
    ResourceRetentionSet rss = new ResourceRetentionSet(delService, TARGET_MB);
    // 3MB files @{10, 15}
    LocalResourcesTracker pubTracker =
      createMockTracker(null, 3 * 1024 * 1024, 2, 10, 5);
    // 1MB files @{3, 6, 9, 12}
    LocalResourcesTracker trackerA =
      createMockTracker("A", 1 * 1024 * 1024, 4, 3, 3);
    // 4MB file @{1}
    LocalResourcesTracker trackerB =
      createMockTracker("B", 4 * 1024 * 1024, 1, 10, 5);
    // 2MB files @{7, 9, 11}
    LocalResourcesTracker trackerC =
      createMockTracker("C", 2 * 1024 * 1024, 3, 7, 2);
    // Total cache: 20MB; verify removed at least 10MB
    rss.addResources(pubTracker);
    rss.addResources(trackerA);
    rss.addResources(trackerB);
    rss.addResources(trackerC);
    long deleted = 0L;
    ArgumentCaptor<LocalizedResource> captor =
      ArgumentCaptor.forClass(LocalizedResource.class);
    verify(pubTracker, atMost(2))
      .remove(captor.capture(), isA(DeletionService.class));
    verify(trackerA, atMost(4))
      .remove(captor.capture(), isA(DeletionService.class));
    verify(trackerB, atMost(1))
      .remove(captor.capture(), isA(DeletionService.class));
    verify(trackerC, atMost(3))
      .remove(captor.capture(), isA(DeletionService.class));
    for (LocalizedResource rem : captor.getAllValues()) {
      deleted += rem.getSize();
    }
    assertTrue(deleted >= 10 * 1024 * 1024);
    assertTrue(deleted < 15 * 1024 * 1024);
  }

  LocalResourcesTracker createMockTracker(String user, final long rsrcSize,
      long nRsrcs, long timestamp, long tsstep) {
    Configuration conf = new Configuration();
    ConcurrentMap<LocalResourceRequest,LocalizedResource> trackerResources =
      new ConcurrentHashMap<LocalResourceRequest,LocalizedResource>();
    LocalResourcesTracker ret = spy(new LocalResourcesTrackerImpl(user, null,
      null, trackerResources, false, conf, new NMNullStateStoreService()));
    for (int i = 0; i < nRsrcs; ++i) {
      final LocalResourceRequest req = new LocalResourceRequest(
          new Path("file:///" + user + "/rsrc" + i), timestamp + i * tsstep,
          LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, null);
      final long ts = timestamp + i * tsstep;
      final Path p = new Path("file:///local/" + user + "/rsrc" + i);
      LocalizedResource rsrc = new LocalizedResource(req, null) {
        @Override public int getRefCount() { return 0; }
        @Override public long getSize() { return rsrcSize; }
        @Override public Path getLocalPath() { return p; }
        @Override public long getTimestamp() { return ts; }
        @Override
        public ResourceState getState() { return ResourceState.LOCALIZED; }
      };
      trackerResources.put(req, rsrc);
    }
    return ret;
  }

}
