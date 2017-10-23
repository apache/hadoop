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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.sun.jersey.api.ConflictException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * @param webApp the address of the remote webap
   * @param hsr the servlet request
   * @param returnType the return type of the REST call
   * @param <T> Type of return object.
   * @param method the HTTP method of the REST call
   * @param targetPath additional path to add to the webapp address
   * @param formParam the form parameters as input for a specific REST call
   * @param additionalParam the query parameters as input for a specific REST
   *          call in case the call has no servlet request
   * @return the retrieved entity from the REST call
   */
  protected static <T> T genericForward(String webApp, HttpServletRequest hsr,
      final Class<T> returnType, HTTPMethods method, String targetPath,
      Object formParam, Map<String, String[]> additionalParam) {

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

          ClientResponse response = RouterWebServiceUtil.invokeRMWebService(
              webApp, targetPath, method,
              (hsr == null) ? null : hsr.getPathInfo(), paramMap, formParam);
          if (Response.class.equals(returnType)) {
            return (T) RouterWebServiceUtil.clientResponseToResponse(response);
          }
          // YARN RM can answer with Status.OK or it throws an exception
          if (response.getStatus() == 200) {
            return response.getEntity(returnType);
          }
          RouterWebServiceUtil.retrieveException(response);
          return null;
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
   *
   * @param additionalParam
   */
  private static ClientResponse invokeRMWebService(String webApp, String path,
      HTTPMethods method, String additionalPath,
      Map<String, String[]> queryParams, Object formParam) {
    Client client = Client.create();

    WebResource webResource = client.resource(webApp).path(path);

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

    // I can forward the call in JSON or XML since the Router will convert it
    // again in Object before send it back to the client
    Builder builder = null;
    if (formParam != null) {
      builder = webResource.entity(formParam, MediaType.APPLICATION_XML);
      builder = builder.accept(MediaType.APPLICATION_XML);
    } else {
      builder = webResource.accept(MediaType.APPLICATION_XML);
    }

    ClientResponse response = null;

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
   * to merge the application reports from the reacheable SubClusters. Via
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

    Map<String, AppInfo> federationAM = new HashMap<String, AppInfo>();
    Map<String, AppInfo> federationUAMSum = new HashMap<String, AppInfo>();
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

    allApps.addAll(new ArrayList<AppInfo>(federationAM.values()));
    return allApps;
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
    nodesInfo.addAll(new ArrayList<NodeInfo>(nodesMap.values()));
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

}
