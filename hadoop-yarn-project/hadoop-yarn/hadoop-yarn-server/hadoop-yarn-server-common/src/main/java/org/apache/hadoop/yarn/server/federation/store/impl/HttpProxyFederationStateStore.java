/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.StringKeyIgnoreCaseMultivaluedMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.federation.store.records.dao.ApplicationHomeSubClusterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.ApplicationsHomeSubClusterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterDeregisterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterHeartbeatDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterIdDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterInfoDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterPoliciesConfigurationsDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterPolicyConfigurationDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClustersInfoDAO;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.ws.http.HTTPException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * REST implementation of {@link FederationStateStore}.
 */
public class HttpProxyFederationStateStore implements FederationStateStore {
  public static final Logger LOG =
      LoggerFactory.getLogger(HttpProxyFederationStateStore.class);

  private String webAppUrl;
  private String webAppResourceRootPath;
  private final Clock clock = new MonotonicClock();
  private Map<SubClusterId, SubClusterInfo> subClusters;

  // It is very expensive to create the client
  // Jersey will spawn a thread for every client request
  private Client client = null;

  public final static String FEDERATION_DEFAULT_SUB_CLUSTERS =
      "yarn.federation.default.subclusters";

  public final static String FEDERATION_STATE_STORE_FALLBACK_ENABLED =
      "yarn.federation.statestore.fallback.enabled";

  public final static boolean DEFAULT_FEDERATION_STATE_STORE_TIMEOUT_ENABLED =
      false;

  private boolean fallbackToConfig;

  @Override
  public void init(Configuration conf) throws YarnException {
    this.webAppUrl = WebAppUtils.HTTP_PREFIX + conf
        .get(YarnConfiguration.FEDERATION_STATESTORE_HTTP_PROXY_URL,
            YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HTTP_PROXY_URL);

    this.webAppResourceRootPath = HttpProxyFederationStateStoreConsts.ROOT;
    this.client = createJerseyClient(conf);
    this.subClusters = new ConcurrentHashMap<>();
    this.fallbackToConfig =
        conf.getBoolean(FEDERATION_STATE_STORE_FALLBACK_ENABLED,
            DEFAULT_FEDERATION_STATE_STORE_TIMEOUT_ENABLED);
    loadSubClustersFromConfig(conf);
  }

  protected void loadSubClustersFromConfig(Configuration conf)
      throws YarnException {
    String str = conf.get(FEDERATION_DEFAULT_SUB_CLUSTERS);
    if (str == null) {
      throw new YarnException("Default subclusters not configured in "
          + FEDERATION_DEFAULT_SUB_CLUSTERS);
    }
    Iterator<String> iter = StringUtils.getStringCollection(str).iterator();
    while (iter.hasNext()) {
      SubClusterId scId = SubClusterId.newInstance(iter.next().trim());
      String rmAddr = iter.next().trim();
      String amRmPort = iter.next().trim();
      String clientRmPort = iter.next().trim();
      String adminPort = iter.next().trim();
      String webPort = iter.next().trim();
      subClusters.put(scId, SubClusterInfo
          .newInstance(scId, rmAddr + ":" + amRmPort,
              rmAddr + ":" + clientRmPort, rmAddr + ":" + adminPort,
              rmAddr + ":" + webPort, 0, SubClusterState.SC_RUNNING, 0, ""));
      LOG.info("Loaded subcluster from config: {}", subClusters.get(scId));
    }
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest request) throws YarnException {

    // Input validator
    FederationMembershipStateStoreInputValidator.validate(request);

    SubClusterInfoDAO scInfoDAO =
        new SubClusterInfoDAO(request.getSubClusterInfo());

    try {
      invokePost(HttpProxyFederationStateStoreConsts.PATH_REGISTER, scInfoDAO,
          null);
    } catch (HTTPException e) {
      LOG.error("registerSubCluster REST call failed ", e);
      return null;
    }

    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest request) throws YarnException {
    // Input validator
    FederationMembershipStateStoreInputValidator.validate(request);

    try {
      invokePost(HttpProxyFederationStateStoreConsts.PATH_DEREGISTER,
          new SubClusterDeregisterDAO(request), null);
    } catch (HTTPException e) {
      LOG.error("deregisterSubCluster REST call failed ", e);
      return null;
    }

    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest request) throws YarnException {
    // Input validator
    FederationMembershipStateStoreInputValidator.validate(request);

    try {
      invokePost(HttpProxyFederationStateStoreConsts.PATH_HEARTBEAT,
          new SubClusterHeartbeatDAO(request), null);
    } catch (HTTPException e) {
      LOG.error("subClusterHeartbeat REST call failed ", e);
      return null;
    }

    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest subClusterRequest) throws YarnException {
    // Input validator
    FederationMembershipStateStoreInputValidator.validate(subClusterRequest);

    SubClusterId scId = subClusterRequest.getSubClusterId();
    String scIdStr = scId.getId();
    SubClusterInfoDAO scInfoDao = null;

    long startTime = clock.getTime();

    try {
      scInfoDao = invokeGet(
          HttpProxyFederationStateStoreConsts.PATH_SUBCLUSTERS + "/" + scIdStr,
          null, SubClusterInfoDAO.class);
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(clock.getTime() - startTime);
    } catch (HTTPException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the SubCluster information for " + scIdStr, e);
    }

    if (scInfoDao == null) {
      LOG.warn("The queried SubCluster: {} does not exist.", scIdStr);
      return null;
    }

    SubClusterInfo scInfo = scInfoDao.toSubClusterInfo();

    try {
      FederationMembershipStateStoreInputValidator.checkSubClusterInfo(scInfo);
    } catch (FederationStateStoreInvalidInputException e) {
      String errMsg = "SubCluster " + scIdStr + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    return GetSubClusterInfoResponse.newInstance(scInfo);
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest subClustersRequest) throws YarnException {

    boolean filterInactiveSubClusters =
        subClustersRequest.getFilterInactiveSubClusters();

    MultivaluedMap<String, String> queryParams =
        new StringKeyIgnoreCaseMultivaluedMap<String>();
    queryParams.add(HttpProxyFederationStateStoreConsts.QUERY_SC_FILTER,
        Boolean.toString(filterInactiveSubClusters));

    SubClustersInfoDAO scsInfoDao = null;

    long startTime = clock.getTime();

    try {
      scsInfoDao =
          invokeGet(HttpProxyFederationStateStoreConsts.PATH_SUBCLUSTERS,
              queryParams, SubClustersInfoDAO.class);
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(clock.getTime() - startTime);
    } catch (Exception e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      if (fallbackToConfig) {
        List<SubClusterInfo> subClustersInfo = new ArrayList<>();
        subClustersInfo.addAll(subClusters.values());
        return GetSubClustersInfoResponse.newInstance(subClustersInfo);
      }
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the information for all the SubClusters ", e);
    }

    List<SubClusterInfo> scInfoList = new ArrayList<>();

    if (scsInfoDao != null) {
      for (SubClusterInfoDAO scInfoDao : scsInfoDao.subClusters) {
        SubClusterInfo scInfo = scInfoDao.toSubClusterInfo();
        try {
          FederationMembershipStateStoreInputValidator
              .checkSubClusterInfo(scInfo);
        } catch (FederationStateStoreInvalidInputException e) {
          String errMsg = "SubCluster " + scInfo.getSubClusterId().toString()
              + " is not valid";
          FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
        }
        scInfoList.add(scInfo);
      }
    }

    return GetSubClustersInfoResponse.newInstance(scInfoList);
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {

    SubClusterPoliciesConfigurationsDAO scPoliciesConfDao = null;

    long startTime = clock.getTime();

    try {
      scPoliciesConfDao =
          invokeGet(HttpProxyFederationStateStoreConsts.PATH_POLICY, null,
              SubClusterPoliciesConfigurationsDAO.class);
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(clock.getTime() - startTime);
    } catch (HTTPException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to obtain the policy information for all the queues.", e);
    }

    List<SubClusterPolicyConfiguration> scPoliciesList = new ArrayList<>();

    if (scPoliciesConfDao != null) {
      for (SubClusterPolicyConfigurationDAO scPolicyDao : scPoliciesConfDao.policies) {
        scPoliciesList.add(scPolicyDao.toSubClusterPolicyConfiguration());
      }
    }

    return GetSubClusterPoliciesConfigurationsResponse
        .newInstance(scPoliciesList);
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {

    // Input validator
    FederationPolicyStoreInputValidator.validate(request);

    String queueName = request.getQueue();
    SubClusterPolicyConfigurationDAO scPolicyConfDao = null;

    long startTime = clock.getTime();

    try {
      scPolicyConfDao = invokeGet(
          HttpProxyFederationStateStoreConsts.PATH_POLICY + "/" + queueName,
          null, SubClusterPolicyConfigurationDAO.class);
      FederationStateStoreClientMetrics
          .succeededStateStoreCall(clock.getTime() - startTime);
    } catch (HTTPException e) {
      FederationStateStoreClientMetrics.failedStateStoreCall();
      FederationStateStoreUtils.logAndThrowRetriableException(LOG,
          "Unable to select the policy for the queue :" + queueName, e);
    }

    if (scPolicyConfDao == null) {
      LOG.warn("Policy for queue: {} does not exist.", queueName);
      return null;
    }

    SubClusterPolicyConfiguration scPolicy =
        scPolicyConfDao.toSubClusterPolicyConfiguration();

    return GetSubClusterPolicyConfigurationResponse.newInstance(scPolicy);
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {
    // Input validator
    FederationPolicyStoreInputValidator.validate(request);

    try {
      invokePost(HttpProxyFederationStateStoreConsts.PATH_POLICY,
          new SubClusterPolicyConfigurationDAO(
              request.getPolicyConfiguration()), null);
    } catch (HTTPException e) {
      LOG.error("setPolicyConfiguration REST call failed ", e);
      return null;
    }

    return SetSubClusterPolicyConfigurationResponse.newInstance();
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {
    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    SubClusterIdDAO scIdDAO;

    try {
      scIdDAO = invokePost(HttpProxyFederationStateStoreConsts.PATH_APP_HOME,
          new ApplicationHomeSubClusterDAO(request), SubClusterIdDAO.class);
    } catch (HTTPException e) {
      LOG.error("addApplicationHomeSubCluster REST call failed ", e);
      return null;
    }

    return AddApplicationHomeSubClusterResponse
        .newInstance(scIdDAO.toSubClusterId());
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {
    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    try {
      invokePut(HttpProxyFederationStateStoreConsts.PATH_APP_HOME,
          new ApplicationHomeSubClusterDAO(request));
    } catch (HTTPException e) {
      LOG.error("updateApplicationHomeSubCluster REST call failed ", e);
      return null;
    }

    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    String appId = request.getApplicationId().toString();

    SubClusterIdDAO scIdDAO;
    try {
      scIdDAO = invokeGet(
          HttpProxyFederationStateStoreConsts.PATH_APP_HOME + "/" + appId, null,
          SubClusterIdDAO.class);
    } catch (HTTPException e) {
      LOG.error("getApplicationHomeSubCluster REST call failed ", e);
      return null;
    }

    if (scIdDAO == null) {
      return null;
    }

    return GetApplicationHomeSubClusterResponse.newInstance(
        ApplicationHomeSubCluster
            .newInstance(request.getApplicationId(), scIdDAO.toSubClusterId()));
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    ApplicationsHomeSubClusterDAO ret;
    try {
      ret = invokeGet(HttpProxyFederationStateStoreConsts.PATH_APP_HOME, null,
          ApplicationsHomeSubClusterDAO.class);
    } catch (HTTPException e) {
      LOG.error("getApplicationsHomeSubCluster REST call failed ", e);
      return null;
    }

    return GetApplicationsHomeSubClusterResponse
        .newInstance(ret.toApplicationsHome());
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {
    // Input validator
    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    String appId = request.getApplicationId().toString();

    try {
      invokeDelete(
          HttpProxyFederationStateStoreConsts.PATH_APP_HOME + "/" + appId);
    } catch (HTTPException e) {
      LOG.error("deleteApplicationHomeSubCluster REST call failed ", e);
      return null;
    }

    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public Version getCurrentVersion() {
    throw new NotImplementedException("Code not implemented.");
  }

  @Override
  public Version loadVersion() {
    throw new NotImplementedException("Code not implemented.");
  }

  private <T> T invokeGet(String path,
      MultivaluedMap<String, String> queryParams, final Class<T> responseClass)
      throws YarnException {
    return invokeFederationStateStoreProxy(webAppUrl, WebApp.HTTP.GET, path,
        queryParams, null, responseClass);
  }

  private <T> T invokeDelete(String path) throws YarnException {
    return invokeFederationStateStoreProxy(webAppUrl, WebApp.HTTP.DELETE, path,
        null, null, null);
  }

  private <T> T invokePost(String path, Object body,
      final Class<T> responseClass) throws YarnException {
    return invokeFederationStateStoreProxy(webAppUrl, WebApp.HTTP.POST, path,
        null, body, responseClass);
  }

  private <T> T invokePut(String path, Object body) throws YarnException {
    return invokeFederationStateStoreProxy(webAppUrl, WebApp.HTTP.PUT, path,
        null, body, null);
  }

  private <T> T invokeFederationStateStoreProxy(String webApp,
      WebApp.HTTP method, String path,
      MultivaluedMap<String, String> queryParams, Object body,
      final Class<T> responseClass) throws YarnException {
    T obj = null;

    WebResource webResource = client.resource(webApp);

    if (queryParams != null && queryParams.size() > 0) {
      webResource = webResource.queryParams(queryParams);
    }

    webResource = webResource.path(webAppResourceRootPath).path(path);

    WebResource.Builder builder = webResource.accept(MediaType.APPLICATION_XML);

    LOG.debug("Invoking HTTP REST request with method " + method.name()
        + " to resource: " + builder.toString());
    ClientResponse response = null;
    try {
      switch (method) {
      case GET:
        response = builder.get(ClientResponse.class);
        break;
      case PUT:
        response = builder.put(ClientResponse.class, body);
        break;
      case POST:
        response = builder.post(ClientResponse.class, body);
        break;
      case DELETE:
        response = builder.delete(ClientResponse.class);
        break;
      default:
        throw new YarnRuntimeException(
            "Unknown HTTP method type " + method.name());
      }

      if (response.getStatus() == 200) {
        if (responseClass != null) {
          obj = response.getEntity(responseClass);
        }
      } else if (response.getStatus() == 404) {
        return null;
      } else {
        throw new FederationStateStoreException(
            response.getEntity(RemoteExceptionData.class).getMessage());
      }
      return obj;
    } finally {
      if (response != null) {
        response.close();
      }
      if (client != null) {
        client.destroy();
      }
    }
  }

  /**
   * Create a Jersey client instance.
   */
  private Client createJerseyClient(Configuration conf) {
    Client client = Client.create();
    client.setConnectTimeout(conf.getInt(
        YarnConfiguration.FEDERATION_STATESTORE_HTTP_PROXY_CONNECT_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HTTP_PROXY_CONNECT_TIMEOUT_MS));
    client.setReadTimeout(conf.getInt(
        YarnConfiguration.FEDERATION_STATESTORE_HTTP_PROXY_READ_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HTTP_PROXY_READ_TIMEOUT_MS));
    return client;
  }
}
