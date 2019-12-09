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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalCacheCleaner.LocalCacheCleanerStats;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.junit.Test;

/**
 * This class tests the clean up of local caches the node manager uses for the
 * purpose of resource localization.
 */
public class TestLocalCacheCleanup {

  @Test
  public void testBasicCleanup() {
    ConcurrentMap<LocalResourceRequest, LocalizedResource> publicRsrc =
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
    addResource(publicRsrc, "/pub-resource1.txt", 5, 20, 0);
    addResource(publicRsrc, "/pub-resource2.txt", 3, 20, 0);
    addResource(publicRsrc, "/pub-resource3.txt", 15, 20, 0);

    ConcurrentMap<String, LocalResourcesTracker> privateRsrc =
        new ConcurrentHashMap<String, LocalResourcesTracker>();

    ConcurrentMap<LocalResourceRequest, LocalizedResource> user1rsrcs =
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
    addResource(user1rsrcs, "/private-u1-resource4.txt", 1, 20, 0);
    LocalResourcesTracker user1Tracker =
        new StubbedLocalResourcesTrackerImpl("user1", user1rsrcs);
    privateRsrc.put("user1", user1Tracker);

    ConcurrentMap<LocalResourceRequest, LocalizedResource> user2rsrcs =
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
    addResource(user2rsrcs, "/private-u2-resource5.txt", 2, 20, 0);
    LocalResourcesTracker user2Tracker =
        new StubbedLocalResourcesTrackerImpl("user2", user2rsrcs);
    privateRsrc.put("user2", user2Tracker);

    ResourceLocalizationService rls =
        createLocService(publicRsrc, privateRsrc, 0);
    LocalCacheCleanerStats stats = rls.handleCacheCleanup();
    assertEquals(0, ((StubbedLocalResourcesTrackerImpl) rls.publicRsrc)
        .getLocalRsrc().size());
    assertEquals(0,
        ((StubbedLocalResourcesTrackerImpl) privateRsrc.get("user1"))
            .getLocalRsrc().size());
    assertEquals(0,
        ((StubbedLocalResourcesTrackerImpl) privateRsrc.get("user2"))
            .getLocalRsrc().size());
    assertEquals(100, stats.getTotalDelSize());
    assertEquals(100, rls.metrics.getTotalBytesDeleted());
    assertEquals(60, stats.getPublicDelSize());
    assertEquals(60, rls.metrics.getPublicBytesDeleted());
    assertEquals(40, stats.getPrivateDelSize());
    assertEquals(40, rls.metrics.getPrivateBytesDeleted());
    assertEquals(100, rls.metrics.getCacheSizeBeforeClean());
  }

  @Test
  public void testPositiveRefCount() {
    ConcurrentMap<LocalResourceRequest, LocalizedResource> publicRsrc =
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
    // Oldest resource with a positive ref count the other with a ref count
    // equal to 0.
    LocalResourceRequest survivor =
        addResource(publicRsrc, "/pub-resource1.txt", 1, 20, 1);
    addResource(publicRsrc, "/pub-resource2.txt", 5, 20, 0);

    ConcurrentMap<String, LocalResourcesTracker> privateRsrc =
        new ConcurrentHashMap<String, LocalResourcesTracker>();

    ResourceLocalizationService rls =
        createLocService(publicRsrc, privateRsrc, 0);
    LocalCacheCleanerStats stats = rls.handleCacheCleanup();
    StubbedLocalResourcesTrackerImpl resources =
        (StubbedLocalResourcesTrackerImpl) rls.publicRsrc;
    assertEquals(1, resources.getLocalRsrc().size());
    assertTrue(resources.getLocalRsrc().containsKey(survivor));
    assertEquals(20, stats.getTotalDelSize());
    assertEquals(20, rls.metrics.getTotalBytesDeleted());
    assertEquals(20, stats.getPublicDelSize());
    assertEquals(20, rls.metrics.getPublicBytesDeleted());
    assertEquals(0, stats.getPrivateDelSize());
    assertEquals(0, rls.metrics.getPrivateBytesDeleted());
    assertEquals(40, rls.metrics.getCacheSizeBeforeClean());
  }

  @Test
  public void testLRUAcrossTrackers() {
    ConcurrentMap<LocalResourceRequest, LocalizedResource> publicRsrc =
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
    LocalResourceRequest pubSurviver1 =
        addResource(publicRsrc, "/pub-resource1.txt", 8, 20, 0);
    LocalResourceRequest pubSurviver2 =
        addResource(publicRsrc, "/pub-resource2.txt", 7, 20, 0);
    addResource(publicRsrc, "/pub-resource3.txt", 1, 20, 0);

    ConcurrentMap<String, LocalResourcesTracker> privateRsrc =
        new ConcurrentHashMap<String, LocalResourcesTracker>();

    ConcurrentMap<LocalResourceRequest, LocalizedResource> user1rsrcs =
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
    LocalResourceRequest usr1Surviver1 =
        addResource(user1rsrcs, "/private-u1-resource1.txt", 6, 20, 0);
    addResource(user1rsrcs, "/private-u1-resource2.txt", 2, 20, 0);
    LocalResourcesTracker user1Tracker =
        new StubbedLocalResourcesTrackerImpl("user1", user1rsrcs);
    privateRsrc.put("user1", user1Tracker);

    ConcurrentMap<LocalResourceRequest, LocalizedResource> user2rsrcs =
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
    LocalResourceRequest usr2Surviver1 =
        addResource(user2rsrcs, "/private-u2-resource1.txt", 5, 20, 0);
    addResource(user2rsrcs, "/private-u2-resource2.txt", 3, 20, 0);
    addResource(user2rsrcs, "/private-u2-resource3.txt", 4, 20, 0);
    LocalResourcesTracker user2Tracker =
        new StubbedLocalResourcesTrackerImpl("user2", user2rsrcs);
    privateRsrc.put("user2", user2Tracker);

    ResourceLocalizationService rls =
        createLocService(publicRsrc, privateRsrc, 80);
    LocalCacheCleanerStats stats = rls.handleCacheCleanup();

    Map<LocalResourceRequest, LocalizedResource> pubLocalRsrc =
        ((StubbedLocalResourcesTrackerImpl) rls.publicRsrc).getLocalRsrc();
    assertEquals(2, pubLocalRsrc.size());
    assertTrue(pubLocalRsrc.containsKey(pubSurviver1));
    assertTrue(pubLocalRsrc.containsKey(pubSurviver2));

    Map<LocalResourceRequest, LocalizedResource> usr1LocalRsrc =
        ((StubbedLocalResourcesTrackerImpl) privateRsrc.get("user1"))
            .getLocalRsrc();
    assertEquals(1, usr1LocalRsrc.size());
    assertTrue(usr1LocalRsrc.containsKey(usr1Surviver1));

    Map<LocalResourceRequest, LocalizedResource> usr2LocalRsrc =
        ((StubbedLocalResourcesTrackerImpl) privateRsrc.get("user2"))
            .getLocalRsrc();
    assertEquals(1, usr2LocalRsrc.size());
    assertTrue(usr2LocalRsrc.containsKey(usr2Surviver1));

    assertEquals(80, stats.getTotalDelSize());
    assertEquals(80, rls.metrics.getTotalBytesDeleted());
    assertEquals(20, stats.getPublicDelSize());
    assertEquals(20, rls.metrics.getPublicBytesDeleted());
    assertEquals(60, stats.getPrivateDelSize());
    assertEquals(60, rls.metrics.getPrivateBytesDeleted());
    assertEquals(160, rls.metrics.getCacheSizeBeforeClean());
  }

  private ResourceLocalizationService createLocService(
      ConcurrentMap<LocalResourceRequest, LocalizedResource> publicRsrcs,
      ConcurrentMap<String, LocalResourcesTracker> privateRsrcs,
      long targetCacheSize) {
    Context mockedContext = mock(Context.class);
    when(mockedContext.getNMStateStore()).thenReturn(null);
    NodeManagerMetrics metrics = NodeManagerMetrics.create();
    ResourceLocalizationService rls =
        new ResourceLocalizationService(null, null, null, null, mockedContext,
            metrics);
    // We set the following members directly so we don't have to deal with
    // mocking out the service init method.
    rls.publicRsrc = new StubbedLocalResourcesTrackerImpl(null, publicRsrcs);
    rls.cacheTargetSize = targetCacheSize;
    rls.privateRsrc.putAll(privateRsrcs);
    return rls;
  }

  private LocalResourceRequest addResource(
      ConcurrentMap<LocalResourceRequest, LocalizedResource> resources,
      String path, long timestamp, long size, int refCount) {
    LocalResourceRequest request = createLocalResourceRequest(path, timestamp);
    LocalizedResource resource =
        createLocalizedResource(size, refCount, timestamp, request);
    resources.put(request, resource);
    return request;
  }

  private LocalResourceRequest createLocalResourceRequest(String path,
      long timestamp) {
    return new LocalResourceRequest(new Path(path), timestamp,
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, null);
  }

  private LocalizedResource createLocalizedResource(long size, int refCount,
      long timestamp, LocalResourceRequest req) {
    LocalizedResource lr = mock(LocalizedResource.class);
    when(lr.getSize()).thenReturn(size);
    when(lr.getRefCount()).thenReturn(refCount);
    when(lr.getTimestamp()).thenReturn(timestamp);
    when(lr.getState()).thenReturn(ResourceState.LOCALIZED);
    when(lr.getRequest()).thenReturn(req);
    return lr;
  }

  class StubbedLocalResourcesTrackerImpl extends LocalResourcesTrackerImpl {
    StubbedLocalResourcesTrackerImpl(String user,
        ConcurrentMap<LocalResourceRequest, LocalizedResource> rsrcs) {
      super(user, null, null, rsrcs, false, new Configuration(), null, null);
    }

    @Override
    public boolean remove(LocalizedResource rem, DeletionService delService) {
      LocalizedResource r = localrsrc.remove(rem.getRequest());
      if (r != null) {
        LOG.info("Removed " + rem.getRequest().getPath()
            + " from localized cache");
        return true;
      }
      return false;
    }

    Map<LocalResourceRequest, LocalizedResource> getLocalRsrc() {
      return Collections.unmodifiableMap(localrsrc);
    }
  }
}
