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

package org.apache.hadoop.yarn.server.router.webapp;

import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices.DELEGATION_TOKEN_HEADER;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.StatisticsItemInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionInfo;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.ConflictException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * The Router webservice util class.
 */
public final class RouterWebServiceUtil {

  private static String user = "YarnRouter";

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterWebServiceUtil.class.getName());

  private final static String PARTIAL_REPORT = "Partial Report ";

  /** Disable constructor. */
  private RouterWebServiceUtil() {
  }

  /**
   * Creates and performs a REST call to a specific WebService.
   *
   * @param webApp the address of the remote webapp
   * @param hsr the servlet request
   * @param returnType the return type of the REST call
   * @param <T> Type of return object.
   * @param method the HTTP method of the REST call
   * @param targetPath additional path to add to the webapp address
   * @param formParam the form parameters as input for a specific REST call
   * @param additionalParam the query parameters as input for a specific REST
   *          call in case the call has no servlet request
   * @param conf configuration.
   * @param client same client used to reduce number of clients created
   * @return the retrieved entity from the REST call
   */
  protected static <T> T genericForward(final String webApp,
      final HttpServletRequest hsr, final Class<T> returnType,
      final HTTPMethods method, final String targetPath, final Object formParam,
      final Map<String, String[]> additionalParam, Configuration conf,
      Client client) {

    UserGroupInformation callerUGI = null;

    if (hsr != null) {
      callerUGI = RMWebAppUtil.getCallerUserGroupInformation(hsr, true);
    } else {
      // user not required
      callerUGI = UserGroupInformation.createRemoteUser(user);

    }

    if (callerUGI == null) {
      LOG.error("Unable to obtain user name, user not authenticated");
      return null;
    }

    try {
      return callerUGI.doAs(new PrivilegedExceptionAction<T>() {
        @SuppressWarnings("unchecked")
        @Override
        public T run() {

          Map<String, String[]> paramMap = null;

          // We can have hsr or additionalParam. There are no case with both.
          if (hsr != null) {
            paramMap = hsr.getParameterMap();
          } else if (additionalParam != null) {
            paramMap = additionalParam;
          }

          ClientResponse response = RouterWebServiceUtil
              .invokeRMWebService(webApp, targetPath, method,
                  (hsr == null) ? null : hsr.getPathInfo(), paramMap, formParam,
                  getMediaTypeFromHttpServletRequest(hsr, returnType), conf,
                  client);
          if (Response.class.equals(returnType)) {
            return (T) RouterWebServiceUtil.clientResponseToResponse(response);
          }

          try {
            // YARN RM can answer with Status.OK or it throws an exception
            if (response.getStatus() == SC_OK) {
              return response.getEntity(returnType);
            }
            if (response.getStatus() == SC_NO_CONTENT) {
              try {
                return returnType.getConstructor().newInstance();
              } catch (RuntimeException | ReflectiveOperationException e) {
                LOG.error("Cannot create empty entity for {}", returnType, e);
              }
            }
            RouterWebServiceUtil.retrieveException(response);
            return null;
          } finally {
            if (response != null) {
              response.close();
            }
          }
        }
      });
    } catch (InterruptedException e) {
      return null;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Performs an invocation of a REST call on a remote RMWebService.
   * @param webApp the address of the remote webapp
   * @param path  to add to the webapp address
   * @param method the HTTP method of the REST call
   * @param additionalPath the servlet request path
   * @param queryParams hsr of additional Param
   * @param formParam the form parameters as input for a specific REST call
   * @param mediaType Media type for Servlet request call
   * @param conf to support http and https
   * @param client same client used to reduce number of clients created
   * @return Client response to REST call
   */
  private static ClientResponse invokeRMWebService(String webApp, String path,
      HTTPMethods method, String additionalPath,
      Map<String, String[]> queryParams, Object formParam, String mediaType,
      Configuration conf, Client client) {
    InetSocketAddress socketAddress = NetUtils
        .getConnectAddress(NetUtils.createSocketAddr(webApp));
    String scheme = YarnConfiguration.useHttps(conf) ? "https://" : "http://";
    String webAddress = scheme + socketAddress.getHostName() + ":"
        + socketAddress.getPort();
    WebResource webResource = client.resource(webAddress).path(path);

    if (additionalPath != null && !additionalPath.isEmpty()) {
      webResource = webResource.path(additionalPath);
    }

    if (queryParams != null && !queryParams.isEmpty()) {
      MultivaluedMap<String, String> paramMap = new MultivaluedMapImpl();

      for (Entry<String, String[]> param : queryParams.entrySet()) {
        String[] values = param.getValue();
        for (int i = 0; i < values.length; i++) {
          paramMap.add(param.getKey(), values[i]);
        }
      }
      webResource = webResource.queryParams(paramMap);
    }

    Builder builder = null;
    if (formParam != null) {
      builder = webResource.entity(formParam, mediaType);
      builder = builder.accept(mediaType);
    } else {
      builder = webResource.accept(mediaType);
    }

    ClientResponse response = null;

    try {
      switch (method) {
      case DELETE:
        response = builder.delete(ClientResponse.class);
        break;
      case GET:
        response = builder.get(ClientResponse.class);
        break;
      case POST:
        response = builder.post(ClientResponse.class);
        break;
      case PUT:
        response = builder.put(ClientResponse.class);
        break;
      default:
        break;
      }
    } finally {
      client.destroy();
    }

    return response;
  }

  public static Response clientResponseToResponse(ClientResponse r) {
    if (r == null) {
      return null;
    }
    // copy the status code
    ResponseBuilder rb = Response.status(r.getStatus());
    // copy all the headers
    for (Entry<String, List<String>> entry : r.getHeaders().entrySet()) {
      for (String value : entry.getValue()) {
        rb.header(entry.getKey(), value);
      }
    }
    // copy the entity
    rb.entity(r.getEntityInputStream());
    // return the response
    return rb.build();
  }

  public static void retrieveException(ClientResponse response) {
    String serverErrorMsg = response.getEntity(String.class);
    int status = response.getStatus();
    if (status == 400) {
      throw new BadRequestException(serverErrorMsg);
    }
    if (status == 403) {
      throw new ForbiddenException(serverErrorMsg);
    }
    if (status == 404) {
      throw new NotFoundException(serverErrorMsg);
    }
    if (status == 409) {
      throw new ConflictException(serverErrorMsg);
    }

  }

  /**
   * Merges a list of AppInfo grouping by ApplicationId. Our current policy is
   * to merge the application reports from the reachable SubClusters. Via
   * configuration parameter, we decide whether to return applications for which
   * the primary AM is missing or to omit them.
   *
   * @param appsInfo a list of AppInfo to merge
   * @param returnPartialResult if the merge AppsInfo should contain partial
   *          result or not
   * @return the merged AppsInfo
   */
  public static AppsInfo mergeAppsInfo(ArrayList<AppInfo> appsInfo,
      boolean returnPartialResult) {
    AppsInfo allApps = new AppsInfo();

    Map<String, AppInfo> federationAM = new HashMap<>();
    Map<String, AppInfo> federationUAMSum = new HashMap<>();
    for (AppInfo a : appsInfo) {
      // Check if this AppInfo is an AM
      if (a.getAMHostHttpAddress() != null) {
        // Insert in the list of AM
        federationAM.put(a.getAppId(), a);
        // Check if there are any UAM found before
        if (federationUAMSum.containsKey(a.getAppId())) {
          // Merge the current AM with the found UAM
          mergeAMWithUAM(a, federationUAMSum.get(a.getAppId()));
          // Remove the sum of the UAMs
          federationUAMSum.remove(a.getAppId());
        }
        // This AppInfo is an UAM
      } else {
        if (federationAM.containsKey(a.getAppId())) {
          // Merge the current UAM with its own AM
          mergeAMWithUAM(federationAM.get(a.getAppId()), a);
        } else if (federationUAMSum.containsKey(a.getAppId())) {
          // Merge the current UAM with its own UAM and update the list of UAM
          federationUAMSum.put(a.getAppId(),
              mergeUAMWithUAM(federationUAMSum.get(a.getAppId()), a));
        } else {
          // Insert in the list of UAM
          federationUAMSum.put(a.getAppId(), a);
        }
      }
    }

    // Check the remaining UAMs are depending or not from federation
    for (AppInfo a : federationUAMSum.values()) {
      if (returnPartialResult || (a.getName() != null
          && !(a.getName().startsWith(UnmanagedApplicationManager.APP_NAME)
              || a.getName().startsWith(PARTIAL_REPORT)))) {
        federationAM.put(a.getAppId(), a);
      }
    }

    allApps.addAll(new ArrayList<>(federationAM.values()));
    return allApps;
  }

  /**
   * Create a Jersey client instance.
   * @param conf Configuration
   * @return a jersey client
   */
  protected static Client createJerseyClient(Configuration conf) {
    Client client = Client.create();

    long checkConnectTimeOut = conf.getLong(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, 0);
    int connectTimeOut = (int) conf.getTimeDuration(YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    if (checkConnectTimeOut <= 0 || checkConnectTimeOut > Integer.MAX_VALUE) {
      LOG.warn("Configuration {} = {} ms error. We will use the default value({} ms).",
          YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, connectTimeOut,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT);
      connectTimeOut = (int) TimeUnit.MILLISECONDS.convert(
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    }
    client.setConnectTimeout(connectTimeOut);

    long checkReadTimeout = conf.getLong(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT, 0);
    int readTimeout = (int) conf.getTimeDuration(YarnConfiguration.ROUTER_WEBAPP_READ_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_READ_TIMEOUT, TimeUnit.MILLISECONDS);

    if (checkReadTimeout < 0) {
      LOG.warn("Configuration {} = {} ms error. We will use the default value({} ms).",
          YarnConfiguration.ROUTER_WEBAPP_CONNECT_TIMEOUT, connectTimeOut,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT);
      readTimeout = (int) TimeUnit.MILLISECONDS.convert(
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    }
    client.setReadTimeout(readTimeout);

    return client;
  }

  private static AppInfo mergeUAMWithUAM(AppInfo uam1, AppInfo uam2) {
    AppInfo partialReport = new AppInfo();
    partialReport.setAppId(uam1.getAppId());
    partialReport.setName(PARTIAL_REPORT + uam1.getAppId());
    // We pick the status of the first uam
    partialReport.setState(uam1.getState());
    // Merge the newly partial AM with UAM1 and then with UAM2
    mergeAMWithUAM(partialReport, uam1);
    mergeAMWithUAM(partialReport, uam2);
    return partialReport;
  }

  private static void mergeAMWithUAM(AppInfo am, AppInfo uam) {
    am.setPreemptedResourceMB(
        am.getPreemptedResourceMB() + uam.getPreemptedResourceMB());
    am.setPreemptedResourceVCores(
        am.getPreemptedResourceVCores() + uam.getPreemptedResourceVCores());
    am.setNumNonAMContainerPreempted(am.getNumNonAMContainerPreempted()
        + uam.getNumNonAMContainerPreempted());
    am.setNumAMContainerPreempted(
        am.getNumAMContainerPreempted() + uam.getNumAMContainerPreempted());
    am.setPreemptedMemorySeconds(
        am.getPreemptedMemorySeconds() + uam.getPreemptedMemorySeconds());
    am.setPreemptedVcoreSeconds(
        am.getPreemptedVcoreSeconds() + uam.getPreemptedVcoreSeconds());

    if (am.getState() == YarnApplicationState.RUNNING
        && uam.getState() == am.getState()) {

      am.getResourceRequests().addAll(uam.getResourceRequests());

      am.setAllocatedMB(am.getAllocatedMB() + uam.getAllocatedMB());
      am.setAllocatedVCores(am.getAllocatedVCores() + uam.getAllocatedVCores());
      am.setReservedMB(am.getReservedMB() + uam.getReservedMB());
      am.setReservedVCores(am.getReservedVCores() + uam.getReservedMB());
      am.setRunningContainers(
          am.getRunningContainers() + uam.getRunningContainers());
      am.setMemorySeconds(am.getMemorySeconds() + uam.getMemorySeconds());
      am.setVcoreSeconds(am.getVcoreSeconds() + uam.getVcoreSeconds());
    }
  }

  /**
   * Deletes all the duplicate NodeInfo by discarding the old instances.
   *
   * @param nodes a list of NodeInfo to check for duplicates
   * @return a NodesInfo that contains a list of NodeInfos without duplicates
   */
  public static NodesInfo deleteDuplicateNodesInfo(ArrayList<NodeInfo> nodes) {
    NodesInfo nodesInfo = new NodesInfo();

    Map<String, NodeInfo> nodesMap = new LinkedHashMap<>();
    for (NodeInfo node : nodes) {
      String nodeId = node.getNodeId();
      // If the node already exists, it could be an old instance
      if (nodesMap.containsKey(nodeId)) {
        // Check if the node is an old instance
        if (nodesMap.get(nodeId).getLastHealthUpdate() < node
            .getLastHealthUpdate()) {
          nodesMap.put(node.getNodeId(), node);
        }
      } else {
        nodesMap.put(node.getNodeId(), node);
      }
    }
    nodesInfo.addAll(new ArrayList<>(nodesMap.values()));
    return nodesInfo;
  }

  /**
   * Adds all the values from the second ClusterMetricsInfo to the first one.
   *
   * @param metrics the ClusterMetricsInfo we want to update
   * @param metricsResponse the ClusterMetricsInfo we want to add to the first
   *          param
   */
  public static void mergeMetrics(ClusterMetricsInfo metrics,
      ClusterMetricsInfo metricsResponse) {
    metrics.setAppsSubmitted(
        metrics.getAppsSubmitted() + metricsResponse.getAppsSubmitted());
    metrics.setAppsCompleted(
        metrics.getAppsCompleted() + metricsResponse.getAppsCompleted());
    metrics.setAppsPending(
        metrics.getAppsPending() + metricsResponse.getAppsPending());
    metrics.setAppsRunning(
        metrics.getAppsRunning() + metricsResponse.getAppsRunning());
    metrics.setAppsFailed(
        metrics.getAppsFailed() + metricsResponse.getAppsFailed());
    metrics.setAppsKilled(
        metrics.getAppsKilled() + metricsResponse.getAppsKilled());

    metrics.setReservedMB(
        metrics.getReservedMB() + metricsResponse.getReservedMB());
    metrics.setAvailableMB(
        metrics.getAvailableMB() + metricsResponse.getAvailableMB());
    metrics.setAllocatedMB(
        metrics.getAllocatedMB() + metricsResponse.getAllocatedMB());

    metrics.setReservedVirtualCores(metrics.getReservedVirtualCores()
        + metricsResponse.getReservedVirtualCores());
    metrics.setAvailableVirtualCores(metrics.getAvailableVirtualCores()
        + metricsResponse.getAvailableVirtualCores());
    metrics.setAllocatedVirtualCores(metrics.getAllocatedVirtualCores()
        + metricsResponse.getAllocatedVirtualCores());

    metrics.setContainersAllocated(metrics.getContainersAllocated()
        + metricsResponse.getContainersAllocated());
    metrics.setContainersReserved(metrics.getReservedContainers()
        + metricsResponse.getReservedContainers());
    metrics.setContainersPending(metrics.getPendingContainers()
        + metricsResponse.getPendingContainers());

    metrics.setTotalMB(metrics.getTotalMB()
        + metricsResponse.getTotalMB());
    metrics.setTotalVirtualCores(metrics.getTotalVirtualCores()
        + metricsResponse.getTotalVirtualCores());
    metrics.setTotalNodes(metrics.getTotalNodes()
        + metricsResponse.getTotalNodes());
    metrics.setLostNodes(metrics.getLostNodes()
        + metricsResponse.getLostNodes());
    metrics.setUnhealthyNodes(metrics.getUnhealthyNodes()
        + metricsResponse.getUnhealthyNodes());
    metrics.setDecommissioningNodes(metrics.getDecommissioningNodes()
        + metricsResponse.getDecommissioningNodes());
    metrics.setDecommissionedNodes(metrics.getDecommissionedNodes()
        + metricsResponse.getDecommissionedNodes());
    metrics.setRebootedNodes(metrics.getRebootedNodes()
        + metricsResponse.getRebootedNodes());
    metrics.setActiveNodes(metrics.getActiveNodes()
        + metricsResponse.getActiveNodes());
    metrics.setShutdownNodes(metrics.getShutdownNodes()
        + metricsResponse.getShutdownNodes());
  }

  /**
   * Extract from HttpServletRequest the MediaType in output.
   *
   * @param request the servlet request.
   * @param returnType the return type of the REST call.
   * @param <T> Generic Type T.
   * @return MediaType.
   */
  protected static <T> String getMediaTypeFromHttpServletRequest(
      HttpServletRequest request, final Class<T> returnType) {
    if (request == null) {
      // By default, we return XML for REST call without HttpServletRequest
      return MediaType.APPLICATION_XML;
    }
    // TODO
    if (!returnType.equals(Response.class)) {
      return MediaType.APPLICATION_XML;
    }
    String header = request.getHeader(HttpHeaders.ACCEPT);
    if (header == null || header.equals("*")) {
      // By default, we return JSON
      return MediaType.APPLICATION_JSON;
    }
    return header;
  }

  public static NodeToLabelsInfo mergeNodeToLabels(
      Map<SubClusterInfo, NodeToLabelsInfo> nodeToLabelsInfoMap) {

    HashMap<String, NodeLabelsInfo> nodeToLabels = new HashMap<>();
    Collection<NodeToLabelsInfo> nodeToLabelsInfos = nodeToLabelsInfoMap.values();

    nodeToLabelsInfos.stream().forEach(nodeToLabelsInfo -> {
      for (Map.Entry<String, NodeLabelsInfo> item : nodeToLabelsInfo.getNodeToLabels().entrySet()) {
        String key = item.getKey();
        NodeLabelsInfo itemValue = item.getValue();
        NodeLabelsInfo nodeToLabelsValue = nodeToLabels.getOrDefault(item.getKey(), null);
        Set<NodeLabel> hashSet = new HashSet<>();
        if (itemValue != null) {
          hashSet.addAll(itemValue.getNodeLabels());
        }
        if (nodeToLabelsValue != null) {
          hashSet.addAll(nodeToLabelsValue.getNodeLabels());
        }
        nodeToLabels.put(key, new NodeLabelsInfo(hashSet));
      }
    });

    return new NodeToLabelsInfo(nodeToLabels);
  }

  public static ApplicationStatisticsInfo mergeApplicationStatisticsInfo(
      Collection<ApplicationStatisticsInfo> appStatistics) {
    ApplicationStatisticsInfo result = new ApplicationStatisticsInfo();
    Map<String, StatisticsItemInfo> statisticsItemMap = new HashMap<>();

    appStatistics.stream().forEach(appStatistic -> {
      List<StatisticsItemInfo> statisticsItemInfos = appStatistic.getStatItems();
      for (StatisticsItemInfo statisticsItemInfo : statisticsItemInfos) {

        String statisticsItemKey =
            statisticsItemInfo.getType() + "_" + statisticsItemInfo.getState().toString();

        StatisticsItemInfo statisticsItemValue;
        if (statisticsItemMap.containsKey(statisticsItemKey)) {
          statisticsItemValue = statisticsItemMap.get(statisticsItemKey);
          long statisticsItemValueCount = statisticsItemValue.getCount();
          long statisticsItemInfoCount = statisticsItemInfo.getCount();
          long newCount = statisticsItemValueCount + statisticsItemInfoCount;
          statisticsItemValue.setCount(newCount);
        } else {
          statisticsItemValue = new StatisticsItemInfo(statisticsItemInfo);
        }

        statisticsItemMap.put(statisticsItemKey, statisticsItemValue);
      }
    });

    if (!statisticsItemMap.isEmpty()) {
      result.getStatItems().addAll(statisticsItemMap.values());
    }

    return result;
  }

  public static NodeLabelsInfo mergeNodeLabelsInfo(Map<SubClusterInfo, NodeLabelsInfo> paramMap) {
    Map<String, NodeLabelInfo> resultMap = new HashMap<>();
    paramMap.values().stream()
        .flatMap(nodeLabelsInfo -> nodeLabelsInfo.getNodeLabelsInfo().stream())
        .forEach(nodeLabelInfo -> {
          String keyLabelName = nodeLabelInfo.getName();
          if (resultMap.containsKey(keyLabelName)) {
            NodeLabelInfo mapNodeLabelInfo = resultMap.get(keyLabelName);
            mapNodeLabelInfo = mergeNodeLabelInfo(mapNodeLabelInfo, nodeLabelInfo);
            resultMap.put(keyLabelName, mapNodeLabelInfo);
          } else {
            resultMap.put(keyLabelName, nodeLabelInfo);
          }
        });
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    nodeLabelsInfo.getNodeLabelsInfo().addAll(resultMap.values());
    return nodeLabelsInfo;
  }

  private static NodeLabelInfo mergeNodeLabelInfo(NodeLabelInfo left, NodeLabelInfo right) {
    NodeLabelInfo resultNodeLabelInfo = new NodeLabelInfo();
    resultNodeLabelInfo.setName(left.getName());

    int newActiveNMs = left.getActiveNMs() + right.getActiveNMs();
    resultNodeLabelInfo.setActiveNMs(newActiveNMs);

    boolean newExclusivity = left.getExclusivity() && right.getExclusivity();
    resultNodeLabelInfo.setExclusivity(newExclusivity);

    PartitionInfo leftPartition = left.getPartitionInfo();
    PartitionInfo rightPartition = right.getPartitionInfo();
    PartitionInfo newPartitionInfo = PartitionInfo.addTo(leftPartition, rightPartition);
    resultNodeLabelInfo.setPartitionInfo(newPartitionInfo);
    return resultNodeLabelInfo;
  }

  /**
   * initForWritableEndpoints does the init and acls verification for all
   * writable REST end points.
   *
   * @param conf Configuration.
   * @param callerUGI remote caller who initiated the request.
   * @throws AuthorizationException in case of no access to perfom this op.
   */
  public static void initForWritableEndpoints(Configuration conf, UserGroupInformation callerUGI)
          throws AuthorizationException {
    if (callerUGI == null) {
      String msg = "Unable to obtain user name, user not authenticated";
      throw new AuthorizationException(msg);
    }

    if (UserGroupInformation.isSecurityEnabled() && isStaticUser(conf, callerUGI)) {
      String msg = "The default static user cannot carry out this operation.";
      throw new ForbiddenException(msg);
    }
  }

  /**
   * Determine whether the user is a static user.
   *
   * @param conf Configuration.
   * @param callerUGI remote caller who initiated the request.
   * @return true, static user; false, not static user;
   */
  private static boolean isStaticUser(Configuration conf, UserGroupInformation callerUGI) {
    String staticUser = conf.get(CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER,
            CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER);
    return staticUser.equals(callerUGI.getUserName());
  }

  public static void createKerberosUserGroupInformation(HttpServletRequest hsr)
          throws YarnException {
    String authType = hsr.getAuthType();

    if (!KerberosAuthenticationHandler.TYPE.equalsIgnoreCase(authType)) {
      String msg = "Delegation token operations can only be carried out on a "
              + "Kerberos authenticated channel. Expected auth type is "
              + KerberosAuthenticationHandler.TYPE + ", got type " + authType;
      throw new YarnException(msg);
    }

    Object ugiAttr =
            hsr.getAttribute(DelegationTokenAuthenticationHandler.DELEGATION_TOKEN_UGI_ATTRIBUTE);
    if (ugiAttr != null) {
      String msg = "Delegation token operations cannot be carried out using "
              + "delegation token authentication.";
      throw new YarnException(msg);
    }
  }

  /**
   * Parse Token data.
   *
   * @param encodedToken tokenData
   * @return RMDelegationTokenIdentifier.
   */
  public static Token<RMDelegationTokenIdentifier> extractToken(String encodedToken) {
    Token<RMDelegationTokenIdentifier> token = new Token<>();
    try {
      token.decodeFromUrlString(encodedToken);
    } catch (Exception ie) {
      throw new BadRequestException("Could not decode encoded token");
    }
    return token;
  }

  public static Token<RMDelegationTokenIdentifier> extractToken(HttpServletRequest request) {
    String encodedToken = request.getHeader(DELEGATION_TOKEN_HEADER);
    if (encodedToken == null) {
      String msg = "Header '" + DELEGATION_TOKEN_HEADER
              + "' containing encoded token not found";
      throw new BadRequestException(msg);
    }
    return extractToken(encodedToken);
  }

  /**
   * Get Kerberos UserGroupInformation.
   *
   * Parse ugi from hsr and set kerberos authentication attributes.
   *
   * @param conf Configuration.
   * @param request the servlet request.
   * @return UserGroupInformation.
   * @throws AuthorizationException if Kerberos auth failed.
   * @throws YarnException If Authentication Type verification fails.
   */
  public static UserGroupInformation getKerberosUserGroupInformation(Configuration conf,
      HttpServletRequest request) throws AuthorizationException, YarnException {
    // Parse ugi from hsr And Check ugi as expected.
    // If ugi is empty or user is a static user, an exception will be thrown.
    UserGroupInformation callerUGI = RMWebAppUtil.getCallerUserGroupInformation(request, true);
    initForWritableEndpoints(conf, callerUGI);

    // Set AuthenticationMethod Kerberos for ugi.
    createKerberosUserGroupInformation(request);
    callerUGI.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);

    // return caller UGI
    return callerUGI;
  }
}
