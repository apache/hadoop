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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FederationJCache extends FederationCache {

  private static final Logger LOG = LoggerFactory.getLogger(FederationJCache.class);

  private Cache<String, CacheRequest<String, ?>> cache;

  private int cacheTimeToLive;

  private boolean isCachingEnabled = false;

  private String className = this.getClass().getSimpleName();

  @Override
  public boolean isCachingEnabled() {
    return isCachingEnabled;
  }

  @Override
  public void initCache(Configuration pConf, FederationStateStore pStateStore) {
    // Picking the JCache provider from classpath, need to make sure there's
    // no conflict or pick up a specific one in the future
    cacheTimeToLive = pConf.getInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_TIME_TO_LIVE_SECS);
    if (cacheTimeToLive <= 0) {
      isCachingEnabled = false;
      return;
    }
    this.setStateStore(pStateStore);
    CachingProvider jcacheProvider = Caching.getCachingProvider();
    CacheManager jcacheManager = jcacheProvider.getCacheManager();
    this.cache = jcacheManager.getCache(className);
    if (this.cache == null) {
      LOG.info("Creating a JCache Manager with name {}.", className);
      Duration cacheExpiry = new Duration(TimeUnit.SECONDS, cacheTimeToLive);
      FactoryBuilder.SingletonFactory<ExpiryPolicy> expiryPolicySingletonFactory =
          new FactoryBuilder.SingletonFactory<>(new CreatedExpiryPolicy(cacheExpiry));
      MutableConfiguration<String, CacheRequest<String, ?>> configuration =
          new MutableConfiguration<>();
      configuration.setStoreByValue(false);
      configuration.setExpiryPolicyFactory(expiryPolicySingletonFactory);
      this.cache = jcacheManager.createCache(className, configuration);
    }
    isCachingEnabled = true;
  }

  @Override
  public void clearCache() {
    CachingProvider jcacheProvider = Caching.getCachingProvider();
    CacheManager jcacheManager = jcacheProvider.getCacheManager();
    jcacheManager.destroyCache(className);
    this.cache = null;
  }

  @Override
  public Map<SubClusterId, SubClusterInfo> getSubClusters(boolean filterInactiveSubClusters)
      throws YarnException {
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(filterInactiveSubClusters));
    CacheRequest<String, ?> cacheRequest = cache.get(cacheKey);
    if (cacheRequest == null) {
      cacheRequest = buildGetSubClustersCacheRequest(className, filterInactiveSubClusters);
      cache.put(cacheKey, cacheRequest);
    }
    return buildSubClusterInfoMap(cacheRequest);
  }

  @Override
  public Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations()
      throws Exception {
    final String cacheKey = buildCacheKey(className, GET_POLICIES_CONFIGURATIONS_CACHEID);
    CacheRequest<String, ?> cacheRequest = cache.get(cacheKey);
    if(cacheRequest == null){
      cacheRequest = buildGetPoliciesConfigurationsCacheRequest(className);
      cache.put(cacheKey, cacheRequest);
    }
    return buildPolicyConfigMap(cacheRequest);
  }

  @Override
  public SubClusterId getApplicationHomeSubCluster(ApplicationId appId)
      throws Exception {
    final String cacheKey = buildCacheKey(className, GET_APPLICATION_HOME_SUBCLUSTER_CACHEID,
        appId.toString());
    CacheRequest<String, ?> cacheRequest = cache.get(cacheKey);
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
    cache.remove(cacheKey);
  }

  @VisibleForTesting
  public Cache<String, CacheRequest<String, ?>> getCache() {
    return cache;
  }

  @VisibleForTesting
  public String getAppHomeSubClusterCacheKey(ApplicationId appId)
      throws YarnException {
    return buildCacheKey(className, GET_APPLICATION_HOME_SUBCLUSTER_CACHEID,
        appId.toString());
  }
}
