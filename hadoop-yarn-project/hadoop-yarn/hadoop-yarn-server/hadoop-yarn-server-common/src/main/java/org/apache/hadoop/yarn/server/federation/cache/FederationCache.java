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
package org.apache.hadoop.yarn.server.federation.cache;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class FederationCache {

  // ------------------------------------ Constants   -------------------------

  protected static final String GET_SUBCLUSTERS_CACHEID = "getSubClusters";

  protected static final String GET_POLICIES_CONFIGURATIONS_CACHEID =
      "getPoliciesConfigurations";
  protected static final String GET_APPLICATION_HOME_SUBCLUSTER_CACHEID =
      "getApplicationHomeSubCluster";

  protected static final String POINT = ".";

  private FederationStateStore stateStore;

  /**
   * Determine whether to enable cache.
   * We judge whether to enable the cache according to the cache time.
   * If the cache time is greater than 0, the cache is enabled.
   * If the cache time is less than or equal 0, the cache is not enabled.
   *
   * @return true, enable cache; false, not enable cache.
   */
  public abstract boolean isCachingEnabled();

  /**
   * Initialize the cache.
   *
   * @param pConf Configuration.
   * @param pStateStore FederationStateStore.
   */
  public abstract void initCache(Configuration pConf, FederationStateStore pStateStore);

  /**
   * clear cache.
   */
  public abstract void clearCache();

  /**
   * Build CacheKey.
   *
   * @param className Cache Class Name.
   * @param methodName Method Name.
   * @return append result.
   * Example: className:FederationJCache, methodName:getPoliciesConfigurations.
   * We Will Return FederationJCache.getPoliciesConfigurations.
   */
  protected String buildCacheKey(String className, String methodName) {
    return buildCacheKey(className, methodName, null);
  }

  /**
   * Build CacheKey.
   *
   * @param className Cache Class Name.
   * @param methodName Method Name.
   * @param argName ArgName.
   * @return append result.
   * Example:
   * className:FederationJCache, methodName:getApplicationHomeSubCluster, argName: app_1
   * We Will Return FederationJCache.getApplicationHomeSubCluster.app_1
   */
  protected String buildCacheKey(String className, String methodName, String argName) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(className).append(POINT).append(methodName);
    if (argName != null) {
      buffer.append(POINT);
      buffer.append(argName);
    }
    return buffer.toString();
  }

  /**
   * Returns the {@link SubClusterInfo} of all active sub cluster(s).
   *
   * @param filterInactiveSubClusters whether to filter out inactive
   *          sub-clusters
   * @return the information of all active sub cluster(s)
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public abstract Map<SubClusterId, SubClusterInfo> getSubClusters(
      boolean filterInactiveSubClusters) throws YarnException;

  /**
   * Get the policies that is represented as
   * {@link SubClusterPolicyConfiguration} for all currently active queues in
   * the system.
   *
   * @return the policies for all currently active queues in the system
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public abstract Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations()
      throws Exception;

  /**
   * Returns the home {@link SubClusterId} for the specified
   * {@link ApplicationId}.
   *
   * @param appId the identifier of the application
   * @return the home sub cluster identifier
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public abstract SubClusterId getApplicationHomeSubCluster(ApplicationId appId) throws Exception;

  /**
   * Remove SubCluster from cache.
   *
   * @param filterInactiveSubClusters whether to filter out inactive
   * sub-clusters.
   */
  public abstract void removeSubCluster(boolean filterInactiveSubClusters);


  // ------------------------------------ SubClustersCache -------------------------

  /**
   * Build GetSubClusters CacheRequest.
   *
   * @param cacheKey cacheKey.
   * @param filterInactiveSubClusters filter Inactive SubClusters.
   * @return CacheRequest.
   * @throws YarnException exceptions from yarn servers.
   */
  protected CacheRequest<String, CacheResponse<SubClusterInfo>> buildGetSubClustersCacheRequest(
      String cacheKey, final boolean filterInactiveSubClusters) throws YarnException {
    CacheResponse<SubClusterInfo> response =
        buildSubClusterInfoResponse(filterInactiveSubClusters);
    CacheRequest<String, CacheResponse<SubClusterInfo>> cacheRequest =
        new CacheRequest<>(cacheKey, response);
    return cacheRequest;
  }

  /**
   * Build SubClusterInfo Response.
   *
   * @param filterInactiveSubClusters whether to filter out inactive sub-clusters.
   * @return SubClusterInfo Response.
   * @throws YarnException exceptions from yarn servers.
   */
  private CacheResponse<SubClusterInfo> buildSubClusterInfoResponse(
      final boolean filterInactiveSubClusters) throws YarnException {
    GetSubClustersInfoRequest request = GetSubClustersInfoRequest.newInstance(
        filterInactiveSubClusters);
    GetSubClustersInfoResponse subClusters = stateStore.getSubClusters(request);
    CacheResponse<SubClusterInfo> response = new SubClusterInfoCacheResponse();
    response.setList(subClusters.getSubClusters());
    return response;
  }

  /**
   * According to the response, build SubClusterInfoMap.
   *
   * @param response GetSubClustersInfoResponse.
   * @return SubClusterInfoMap.
   */
  public static Map<SubClusterId, SubClusterInfo> buildSubClusterInfoMap(
      final GetSubClustersInfoResponse response) {
    List<SubClusterInfo> subClusters = response.getSubClusters();
    return buildSubClusterInfoMap(subClusters);
  }

  /**
   * According to the cacheRequest, build SubClusterInfoMap.
   *
   * @param cacheRequest CacheRequest.
   * @return SubClusterInfoMap.
   */
  public static Map<SubClusterId, SubClusterInfo> buildSubClusterInfoMap(
      CacheRequest<String, ?> cacheRequest) {
    Object value = cacheRequest.value;
    SubClusterInfoCacheResponse response = SubClusterInfoCacheResponse.class.cast(value);
    List<SubClusterInfo> subClusters = response.getList();
    return buildSubClusterInfoMap(subClusters);
  }

  /**
   * According to the subClusters, build SubClusterInfoMap.
   *
   * @param subClusters subCluster List.
   * @return SubClusterInfoMap.
   */
  private static Map<SubClusterId, SubClusterInfo> buildSubClusterInfoMap(
      List<SubClusterInfo> subClusters) {
    Map<SubClusterId, SubClusterInfo> subClustersMap = new HashMap<>(subClusters.size());
    for (SubClusterInfo subCluster : subClusters) {
      subClustersMap.put(subCluster.getSubClusterId(), subCluster);
    }
    return subClustersMap;
  }

  // ------------------------------------ ApplicationHomeSubClusterCache -------------------------

  /**
   * Build GetApplicationHomeSubCluster CacheRequest.
   *
   * @param cacheKey cacheKey.
   * @param applicationId applicationId.
   * @return CacheRequest.
   * @throws YarnException exceptions from yarn servers.
   */
  protected CacheRequest<String, CacheResponse<SubClusterId>>
      buildGetApplicationHomeSubClusterRequest(String cacheKey, ApplicationId applicationId)
      throws YarnException {
    CacheResponse<SubClusterId> response = buildSubClusterIdResponse(applicationId);
    return new CacheRequest<>(cacheKey, response);
  }

  /**
   * Build SubClusterId Response.
   *
   * @param applicationId applicationId.
   * @return subClusterId
   * @throws YarnException exceptions from yarn servers.
   */
  private CacheResponse<SubClusterId> buildSubClusterIdResponse(final ApplicationId applicationId)
      throws YarnException {
    GetApplicationHomeSubClusterRequest request =
         GetApplicationHomeSubClusterRequest.newInstance(applicationId);
    GetApplicationHomeSubClusterResponse response =
         stateStore.getApplicationHomeSubCluster(request);
    ApplicationHomeSubCluster appHomeSubCluster = response.getApplicationHomeSubCluster();
    SubClusterId subClusterId = appHomeSubCluster.getHomeSubCluster();
    CacheResponse<SubClusterId> cacheResponse = new ApplicationHomeSubClusterCacheResponse();
    cacheResponse.setItem(subClusterId);
    return cacheResponse;
  }

  // ------------------------------ SubClusterPolicyConfigurationCache -------------------------

  /**
   * Build GetPoliciesConfigurations CacheRequest.
   *
   * @param cacheKey cacheKey.
   * @return CacheRequest.
   * @throws YarnException exceptions from yarn servers.
   */
  protected CacheRequest<String, CacheResponse<SubClusterPolicyConfiguration>>
      buildGetPoliciesConfigurationsCacheRequest(String cacheKey) throws YarnException {
    CacheResponse<SubClusterPolicyConfiguration> response =
         buildSubClusterPolicyConfigurationResponse();
    return new CacheRequest<>(cacheKey, response);
  }

  /**
   * According to the response, build PolicyConfigMap.
   *
   * @param response GetSubClusterPoliciesConfigurationsResponse.
   * @return PolicyConfigMap.
   */
  public static Map<String, SubClusterPolicyConfiguration> buildPolicyConfigMap(
      GetSubClusterPoliciesConfigurationsResponse response) {
    List<SubClusterPolicyConfiguration> policyConfigs = response.getPoliciesConfigs();
    return buildPolicyConfigMap(policyConfigs);
  }

  /**
   * According to the subClusters, build PolicyConfigMap.
   *
   * @param policyConfigs SubClusterPolicyConfigurations
   * @return PolicyConfigMap.
   */
  private static Map<String, SubClusterPolicyConfiguration> buildPolicyConfigMap(
      List<SubClusterPolicyConfiguration> policyConfigs) {
    Map<String, SubClusterPolicyConfiguration> queuePolicyConfigs = new HashMap<>();
    for (SubClusterPolicyConfiguration policyConfig : policyConfigs) {
      queuePolicyConfigs.put(policyConfig.getQueue(), policyConfig);
    }
    return queuePolicyConfigs;
  }

  /**
   * According to the cacheRequest, build PolicyConfigMap.
   *
   * @param cacheRequest CacheRequest.
   * @return PolicyConfigMap.
   */
  public static Map<String, SubClusterPolicyConfiguration> buildPolicyConfigMap(
      CacheRequest<String, ?> cacheRequest){
    Object value = cacheRequest.value;
    SubClusterPolicyConfigurationCacheResponse response =
        SubClusterPolicyConfigurationCacheResponse.class.cast(value);
    List<SubClusterPolicyConfiguration> subClusters = response.getList();
    return buildPolicyConfigMap(subClusters);
  }

  /**
   * Build SubClusterPolicyConfiguration Response.
   *
   * @return SubClusterPolicyConfiguration Response.
   * @throws YarnException exceptions from yarn servers.
   */
  private CacheResponse<SubClusterPolicyConfiguration> buildSubClusterPolicyConfigurationResponse()
      throws YarnException {
    GetSubClusterPoliciesConfigurationsRequest request =
        GetSubClusterPoliciesConfigurationsRequest.newInstance();
    GetSubClusterPoliciesConfigurationsResponse response =
        stateStore.getPoliciesConfigurations(request);
    List<SubClusterPolicyConfiguration> policyConfigs = response.getPoliciesConfigs();
    CacheResponse<SubClusterPolicyConfiguration> cacheResponse =
        new SubClusterPolicyConfigurationCacheResponse();
    cacheResponse.setList(policyConfigs);
    return cacheResponse;
  }

  /**
   * Internal class that encapsulates the cache key and a function that returns
   * the value for the specified key.
   */
  public class CacheRequest<K, V> {
    private K key;
    private V value;

    CacheRequest(K pKey, V pValue) {
      this.key = pKey;
      this.value = pValue;
    }

    public V getValue() throws Exception {
      return value;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(key).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null) {
        return false;
      }

      if (obj instanceof CacheRequest) {
        Class<CacheRequest> cacheRequestClass = CacheRequest.class;
        CacheRequest other = cacheRequestClass.cast(obj);
        return new EqualsBuilder().append(key, other.key).isEquals();
      }

      return false;
    }
  }

  public class CacheResponse<R> {
    private List<R> list;

    private R item;

    public List<R> getList() {
      return list;
    }

    public void setList(List<R> list) {
      this.list = list;
    }

    public R getItem() {
      return item;
    }

    public void setItem(R pItem) {
      this.item = pItem;
    }
  }

  public class SubClusterInfoCacheResponse extends CacheResponse<SubClusterInfo> {
    @Override
    public List<SubClusterInfo> getList() {
      return super.getList();
    }

    @Override
    public void setList(List<SubClusterInfo> list) {
      super.setList(list);
    }

    @Override
    public SubClusterInfo getItem() {
      return super.getItem();
    }

    @Override
    public void setItem(SubClusterInfo item) {
      super.setItem(item);
    }
  }

  public class SubClusterPolicyConfigurationCacheResponse
      extends CacheResponse<SubClusterPolicyConfiguration> {
    @Override
    public List<SubClusterPolicyConfiguration> getList() {
      return super.getList();
    }

    @Override
    public void setList(List<SubClusterPolicyConfiguration> list) {
      super.setList(list);
    }

    @Override
    public SubClusterPolicyConfiguration getItem() {
      return super.getItem();
    }

    @Override
    public void setItem(SubClusterPolicyConfiguration item) {
      super.setItem(item);
    }
  }

  public class ApplicationHomeSubClusterCacheResponse
      extends CacheResponse<SubClusterId> {
    @Override
    public List<SubClusterId> getList() {
      return super.getList();
    }

    @Override
    public void setList(List<SubClusterId> list) {
      super.setList(list);
    }

    @Override
    public SubClusterId getItem() {
      return super.getItem();
    }

    @Override
    public void setItem(SubClusterId item) {
      super.setItem(item);
    }
  }

  public FederationStateStore getStateStore() {
    return stateStore;
  }

  public void setStateStore(FederationStateStore stateStore) {
    this.stateStore = stateStore;
  }
}
