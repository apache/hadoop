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

package org.apache.hadoop.yarn.server.federation.store.impl;

import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
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
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterIdPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterInfoPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterPolicyConfigurationPBImpl;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * ZooKeeper implementation of {@link FederationStateStore}.
 *
 * The znode structure is as follows:
 * ROOT_DIR_PATH
 * |--- MEMBERSHIP
 * |     |----- SC1
 * |     |----- SC2
 * |--- APPLICATION
 * |     |----- APP1
 * |     |----- APP2
 * |--- POLICY
 *       |----- QUEUE1
 *       |----- QUEUE1
 */
public class ZookeeperFederationStateStore implements FederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZookeeperFederationStateStore.class);

  private final static String ROOT_ZNODE_NAME_MEMBERSHIP = "memberships";
  private final static String ROOT_ZNODE_NAME_APPLICATION = "applications";
  private final static String ROOT_ZNODE_NAME_POLICY = "policies";

  /** Interface to Zookeeper. */
  private ZKCuratorManager zkManager;

  /** Directory to store the state store data. */
  private String baseZNode;

  private String appsZNode;
  private String membershipZNode;
  private String policiesZNode;

  @Override
  public void init(Configuration conf) throws YarnException {
    LOG.info("Initializing ZooKeeper connection");

    baseZNode = conf.get(
        YarnConfiguration.FEDERATION_STATESTORE_ZK_PARENT_PATH,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_ZK_PARENT_PATH);
    try {
      this.zkManager = new ZKCuratorManager(conf);
      this.zkManager.start();
    } catch (IOException e) {
      LOG.error("Cannot initialize the ZK connection", e);
    }

    // Base znodes
    membershipZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_MEMBERSHIP);
    appsZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_APPLICATION);
    policiesZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_POLICY);

    // Create base znode for each entity
    try {
      List<ACL> zkAcl = ZKCuratorManager.getZKAcls(conf);
      zkManager.createRootDirRecursively(membershipZNode, zkAcl);
      zkManager.createRootDirRecursively(appsZNode, zkAcl);
      zkManager.createRootDirRecursively(policiesZNode, zkAcl);
    } catch (Exception e) {
      String errMsg = "Cannot create base directories: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

  }

  @Override
  public void close() throws Exception {
    if (zkManager != null) {
      zkManager.close();
    }
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationHomeSubCluster app = request.getApplicationHomeSubCluster();
    ApplicationId appId = app.getApplicationId();

    // Try to write the subcluster
    SubClusterId homeSubCluster = app.getHomeSubCluster();
    try {
      putApp(appId, homeSubCluster, false);
    } catch (Exception e) {
      String errMsg = "Cannot add application home subcluster for " + appId;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    // Check for the actual subcluster
    try {
      homeSubCluster = getApp(appId);
    } catch (Exception e) {
      String errMsg = "Cannot check app home subcluster for " + appId;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    return AddApplicationHomeSubClusterResponse
        .newInstance(homeSubCluster);
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse
      updateApplicationHomeSubCluster(
          UpdateApplicationHomeSubClusterRequest request)
              throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationHomeSubCluster app = request.getApplicationHomeSubCluster();
    ApplicationId appId = app.getApplicationId();
    SubClusterId homeSubCluster = getApp(appId);
    if (homeSubCluster == null) {
      String errMsg = "Application " + appId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    SubClusterId newSubClusterId =
        request.getApplicationHomeSubCluster().getHomeSubCluster();
    putApp(appId, newSubClusterId, true);
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId = request.getApplicationId();
    SubClusterId homeSubCluster = getApp(appId);
    if (homeSubCluster == null) {
      String errMsg = "Application " + appId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return GetApplicationHomeSubClusterResponse.newInstance(
        ApplicationHomeSubCluster.newInstance(appId, homeSubCluster));
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    List<ApplicationHomeSubCluster> result = new ArrayList<>();

    try {
      for (String child : zkManager.getChildren(appsZNode)) {
        ApplicationId appId = ApplicationId.fromString(child);
        SubClusterId homeSubCluster = getApp(appId);
        ApplicationHomeSubCluster app =
            ApplicationHomeSubCluster.newInstance(appId, homeSubCluster);
        result.add(app);
      }
    } catch (Exception e) {
      String errMsg = "Cannot get apps: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    return GetApplicationsHomeSubClusterResponse.newInstance(result);
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse
      deleteApplicationHomeSubCluster(
          DeleteApplicationHomeSubClusterRequest request)
              throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId = request.getApplicationId();
    String appZNode = getNodePath(appsZNode, appId.toString());

    boolean exists = false;
    try {
      exists = zkManager.exists(appZNode);
    } catch (Exception e) {
      String errMsg = "Cannot check app: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    if (!exists) {
      String errMsg = "Application " + appId + " does not exist";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    try {
      zkManager.delete(appZNode);
    } catch (Exception e) {
      String errMsg = "Cannot delete app: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest request) throws YarnException {
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterInfo subClusterInfo = request.getSubClusterInfo();
    SubClusterId subclusterId = subClusterInfo.getSubClusterId();

    // Update the heartbeat time
    long currentTime = getCurrentTime();
    subClusterInfo.setLastHeartBeat(currentTime);

    try {
      putSubclusterInfo(subclusterId, subClusterInfo, true);
    } catch (Exception e) {
      String errMsg = "Cannot register subcluster: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest request) throws YarnException {
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterState state = request.getState();

    // Get the current information and update it
    SubClusterInfo subClusterInfo = getSubclusterInfo(subClusterId);
    if (subClusterInfo == null) {
      String errMsg = "SubCluster " + subClusterId + " not found";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    } else {
      subClusterInfo.setState(state);
      putSubclusterInfo(subClusterId, subClusterInfo, true);
    }

    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest request) throws YarnException {

    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();

    SubClusterInfo subClusterInfo = getSubclusterInfo(subClusterId);
    if (subClusterInfo == null) {
      String errMsg = "SubCluster " + subClusterId
          + " does not exist; cannot heartbeat";
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    long currentTime = getCurrentTime();
    subClusterInfo.setLastHeartBeat(currentTime);
    subClusterInfo.setState(request.getState());
    subClusterInfo.setCapability(request.getCapability());

    putSubclusterInfo(subClusterId, subClusterInfo, true);

    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest request) throws YarnException {

    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterInfo subClusterInfo = null;
    try {
      subClusterInfo = getSubclusterInfo(subClusterId);
      if (subClusterInfo == null) {
        LOG.warn("The queried SubCluster: {} does not exist.", subClusterId);
        return null;
      }
    } catch (Exception e) {
      String errMsg = "Cannot get subcluster: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return GetSubClusterInfoResponse.newInstance(subClusterInfo);
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest request) throws YarnException {
    List<SubClusterInfo> result = new ArrayList<>();

    try {
      for (String child : zkManager.getChildren(membershipZNode)) {
        SubClusterId subClusterId = SubClusterId.newInstance(child);
        SubClusterInfo info = getSubclusterInfo(subClusterId);
        if (!request.getFilterInactiveSubClusters() ||
            info.getState().isActive()) {
          result.add(info);
        }
      }
    } catch (Exception e) {
      String errMsg = "Cannot get subclusters: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return GetSubClustersInfoResponse.newInstance(result);
  }


  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {

    FederationPolicyStoreInputValidator.validate(request);
    String queue = request.getQueue();
    SubClusterPolicyConfiguration policy = null;
    try {
      policy = getPolicy(queue);
    } catch (Exception e) {
      String errMsg = "Cannot get policy: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }

    if (policy == null) {
      LOG.warn("Policy for queue: {} does not exist.", queue);
      return null;
    }
    return GetSubClusterPolicyConfigurationResponse
        .newInstance(policy);
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {

    FederationPolicyStoreInputValidator.validate(request);
    SubClusterPolicyConfiguration policy =
        request.getPolicyConfiguration();
    try {
      String queue = policy.getQueue();
      putPolicy(queue, policy, true);
    } catch (Exception e) {
      String errMsg = "Cannot set policy: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return SetSubClusterPolicyConfigurationResponse.newInstance();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    List<SubClusterPolicyConfiguration> result = new ArrayList<>();

    try {
      for (String child : zkManager.getChildren(policiesZNode)) {
        SubClusterPolicyConfiguration policy = getPolicy(child);
        result.add(policy);
      }
    } catch (Exception e) {
      String errMsg = "Cannot get policies: " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return GetSubClusterPoliciesConfigurationsResponse.newInstance(result);
  }

  @Override
  public Version getCurrentVersion() {
    return null;
  }

  @Override
  public Version loadVersion() {
    return null;
  }

  /**
   * Get the subcluster for an application.
   * @param appId Application identifier.
   * @return Subcluster identifier.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private SubClusterId getApp(final ApplicationId appId) throws YarnException {
    String appZNode = getNodePath(appsZNode, appId.toString());

    SubClusterId subClusterId = null;
    byte[] data = get(appZNode);
    if (data != null) {
      try {
        subClusterId = new SubClusterIdPBImpl(
            SubClusterIdProto.parseFrom(data));
      } catch (InvalidProtocolBufferException e) {
        String errMsg = "Cannot parse application at " + appZNode;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
    }
    return subClusterId;
  }

  /**
   * Put an application.
   * @param appId Application identifier.
   * @param subClusterId Subcluster identifier.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private void putApp(final ApplicationId appId,
      final SubClusterId subClusterId, boolean update)
          throws YarnException {
    String appZNode = getNodePath(appsZNode, appId.toString());
    SubClusterIdProto proto =
        ((SubClusterIdPBImpl)subClusterId).getProto();
    byte[] data = proto.toByteArray();
    put(appZNode, data, update);
  }

  /**
   * Get the current information for a subcluster from Zookeeper.
   * @param subclusterId Subcluster identifier.
   * @return Subcluster information or null if it doesn't exist.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private SubClusterInfo getSubclusterInfo(final SubClusterId subclusterId)
      throws YarnException {
    String memberZNode = getNodePath(membershipZNode, subclusterId.toString());

    SubClusterInfo policy = null;
    byte[] data = get(memberZNode);
    if (data != null) {
      try {
        policy = new SubClusterInfoPBImpl(
            SubClusterInfoProto.parseFrom(data));
      } catch (InvalidProtocolBufferException e) {
        String errMsg = "Cannot parse subcluster info at " + memberZNode;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
    }
    return policy;
  }

  /**
   * Put the subcluster information in Zookeeper.
   * @param subclusterId Subcluster identifier.
   * @param subClusterInfo Subcluster information.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  private void putSubclusterInfo(final SubClusterId subclusterId,
      final SubClusterInfo subClusterInfo, final boolean update)
          throws YarnException {
    String memberZNode = getNodePath(membershipZNode, subclusterId.toString());
    SubClusterInfoProto proto =
        ((SubClusterInfoPBImpl)subClusterInfo).getProto();
    byte[] data = proto.toByteArray();
    put(memberZNode, data, update);
  }

  /**
   * Get the queue policy from Zookeeper.
   * @param queue Name of the queue.
   * @return Subcluster policy configuration.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private SubClusterPolicyConfiguration getPolicy(final String queue)
      throws YarnException {
    String policyZNode = getNodePath(policiesZNode, queue);

    SubClusterPolicyConfiguration policy = null;
    byte[] data = get(policyZNode);
    if (data != null) {
      try {
        policy = new SubClusterPolicyConfigurationPBImpl(
            SubClusterPolicyConfigurationProto.parseFrom(data));
      } catch (InvalidProtocolBufferException e) {
        String errMsg = "Cannot parse policy at " + policyZNode;
        FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
      }
    }
    return policy;
  }

  /**
   * Put the subcluster information in Zookeeper.
   * @param queue Name of the queue.
   * @param policy Subcluster policy configuration.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private void putPolicy(final String queue,
      final SubClusterPolicyConfiguration policy, boolean update)
          throws YarnException {
    String policyZNode = getNodePath(policiesZNode, queue);

    SubClusterPolicyConfigurationProto proto =
        ((SubClusterPolicyConfigurationPBImpl)policy).getProto();
    byte[] data = proto.toByteArray();
    put(policyZNode, data, update);
  }

  /**
   * Get data from a znode in Zookeeper.
   * @param znode Path of the znode.
   * @return Data in the znode.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private byte[] get(String znode) throws YarnException {
    boolean exists = false;
    try {
      exists = zkManager.exists(znode);
    } catch (Exception e) {
      String errMsg = "Cannot find znode " + znode;
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    if (!exists) {
      LOG.error("{} does not exist", znode);
      return null;
    }

    byte[] data = null;
    try {
      data = zkManager.getData(znode);
    } catch (Exception e) {
      String errMsg = "Cannot get data from znode " + znode
          + ": " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    return data;
  }

  /**
   * Put data into a znode in Zookeeper.
   * @param znode Path of the znode.
   * @param data Data to write.
   * @throws YarnException If it cannot contact ZooKeeper.
   */
  private void put(String znode, byte[] data, boolean update)
      throws YarnException {
    // Create the znode
    boolean created = false;
    try {
      created = zkManager.create(znode);
    } catch (Exception e) {
      String errMsg = "Cannot create znode " + znode + ": " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
    if (!created) {
      LOG.debug("{} not created", znode);
      if (!update) {
        LOG.info("{} already existed and we are not updating", znode);
        return;
      }
    }

    // Write the data into the znode
    try {
      zkManager.setData(znode, data, -1);
    } catch (Exception e) {
      String errMsg = "Cannot write data into znode " + znode
          + ": " + e.getMessage();
      FederationStateStoreUtils.logAndThrowStoreException(LOG, errMsg);
    }
  }

  /**
   * Get the current time.
   * @return Current time in milliseconds.
   */
  private static long getCurrentTime() {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    return cal.getTimeInMillis();
  }
}
