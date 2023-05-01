/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FederationGuavaCache extends FederationCache {

  private Cache<String, CacheRequest<String, ?>> cache;

  private int cacheTimeToLive;

  private String className = this.getClass().getSimpleName();

  private boolean isCachingEnabled = false;

  @Override
  public boolean isCachingEnabled() {
    return isCachingEnabled;
  }

  @Override
  public void initCache(Configuration pConf, FederationStateStore pStateStore) {
    // Picking the JCache provider from classpath, need to make sure there's
    // no conflict or pick up a specific one in the future.
    cacheTimeToLive = pConf.getInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_TIME_TO_LIVE_SECS);
    if (cacheTimeToLive <= 0) {
      isCachingEnabled = false;
      return;
    }
    this.setStateStore(pStateStore);

    // Initialize Cache.
    cache = CacheBuilder.newBuilder().expireAfterWrite(cacheTimeToLive,
        TimeUnit.MILLISECONDS).build();
    isCachingEnabled = true;
  }

  @Override
  public void clearCache() {
    cache.invalidateAll();
    cache = null;
  }

  @Override
  public Map<SubClusterId, SubClusterInfo> getSubClusters(boolean filterInactiveSubClusters)
      throws YarnException {
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(filterInactiveSubClusters));
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    if (cacheRequest == null) {
      cacheRequest = buildGetSubClustersCacheRequest(className, filterInactiveSubClusters);
      cache.put(cacheKey, cacheRequest);
    }
    return buildSubClusterInfoMap(cacheRequest);
  }

  @Override
  public Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations() throws Exception {
    final String cacheKey = buildCacheKey(className, GET_POLICIES_CONFIGURATIONS_CACHEID);
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    if(cacheRequest == null){
      cacheRequest = buildGetPoliciesConfigurationsCacheRequest(className);
      cache.put(cacheKey, cacheRequest);
    }
    return buildPolicyConfigMap(cacheRequest);
  }

  @Override
  public SubClusterId getApplicationHomeSubCluster(ApplicationId appId) throws Exception {
    final String cacheKey = buildCacheKey(className, GET_APPLICATION_HOME_SUBCLUSTER_CACHEID,
        appId.toString());
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    if (cacheRequest == null) {
      cacheRequest = buildGetApplicationHomeSubClusterRequest(className, appId);
      cache.put(cacheKey, cacheRequest);
    }
    CacheResponse<SubClusterId> response =
        ApplicationHomeSubClusterCacheResponse.class.cast(cacheRequest.getValue());
    return response.getItem();
  }

  @Override
  public void removeSubCluster(boolean flushCache) {
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(flushCache));
    cache.invalidate(cacheKey);
  }
}
