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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

/**
 * SecretManager for ContainerTokens. This is RM-specific and rolls the
 * master-keys every so often.
 * 
 */
public class RMContainerTokenSecretManager extends
    BaseContainerTokenSecretManager {

  private static Log LOG = LogFactory
      .getLog(RMContainerTokenSecretManager.class);

  private MasterKeyData nextMasterKey;

  private final Timer timer;
  private final long rollingInterval;
  private final long activationDelay;

  public RMContainerTokenSecretManager(Configuration conf) {
    super(conf);

    this.timer = new Timer();
    this.rollingInterval = conf.getLong(
            YarnConfiguration.RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) * 1000;
    // Add an activation delay. This is to address the following race: RM may
    // roll over master-key, scheduling may happen at some point of time, a
    // container created with a password generated off new master key, but NM
    // might not have come again to RM to update the shared secret: so AM has a
    // valid password generated off new secret but NM doesn't know about the
    // secret yet.
    // Adding delay = 1.5 * expiry interval makes sure that all active NMs get
    // the updated shared-key.
    this.activationDelay =
        (long) (conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS) * 1.5);
    LOG.info("ContainerTokenKeyRollingInterval: " + this.rollingInterval
        + "ms and ContainerTokenKeyActivationDelay: " + this.activationDelay
        + "ms");
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS
              + " should be more than 3 X "
              + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS);
    }
  }

  public void start() {
    rollMasterKey();
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
        rollingInterval);
  }

  public void stop() {
    this.timer.cancel();
  }

  /**
   * Creates a new master-key and sets it as the primary.
   */
  @Private
  public void rollMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Rolling master-key for container-tokens");
      if (this.currentMasterKey == null) { // Setting up for the first time.
        this.currentMasterKey = createNewMasterKey();
      } else {
        this.nextMasterKey = createNewMasterKey();
        LOG.info("Going to activate master-key with key-id "
            + this.nextMasterKey.getMasterKey().getKeyId() + " in "
            + this.activationDelay + "ms");
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    } finally {
      super.writeLock.unlock();
    }
  }

  @Private
  public MasterKey getNextKey() {
    super.readLock.lock();
    try {
      if (this.nextMasterKey == null) {
        return null;
      } else {
        return this.nextMasterKey.getMasterKey();
      }
    } finally {
      super.readLock.unlock();
    }
  }

  /**
   * Activate the new master-key
   */
  @Private
  public void activateNextMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: "
          + this.nextMasterKey.getMasterKey().getKeyId());
      this.currentMasterKey = this.nextMasterKey;
      this.nextMasterKey = null;
    } finally {
      super.writeLock.unlock();
    }
  }

  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }
  
  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      // Activation will happen after an absolute time interval. It will be good
      // if we can force activation after an NM updates and acknowledges a
      // roll-over. But that is only possible when we move to per-NM keys. TODO:
      activateNextMasterKey();
    }
  }

  @VisibleForTesting
  public Token createContainerToken(ContainerId containerId,
      int containerVersion, NodeId nodeId, String appSubmitter,
      Resource capability, Priority priority, long createTime) {
    return createContainerToken(containerId, containerVersion, nodeId,
        appSubmitter, capability, priority, createTime,
        null, null, ContainerType.TASK,
        ExecutionType.GUARANTEED, -1, null);
  }

  /**
   * Helper function for creating ContainerTokens.
   *
   * @param containerId Container Id
   * @param containerVersion Container version
   * @param nodeId Node Id
   * @param appSubmitter App Submitter
   * @param capability Capability
   * @param priority Priority
   * @param createTime Create Time
   * @param logAggregationContext Log Aggregation Context
   * @param nodeLabelExpression Node Label Expression
   * @param containerType Container Type
   * @param execType Execution Type
   * @param allocationRequestId allocationRequestId
   * @return the container-token
   */
  public Token createContainerToken(ContainerId containerId,
      int containerVersion, NodeId nodeId, String appSubmitter,
      Resource capability, Priority priority, long createTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression,
      ContainerType containerType, ExecutionType execType,
      long allocationRequestId, Set<String> allocationTags) {
    byte[] password;
    ContainerTokenIdentifier tokenIdentifier;
    long expiryTimeStamp =
        System.currentTimeMillis() + containerTokenExpiryInterval;

    // Lock so that we use the same MasterKey's keyId and its bytes
    this.readLock.lock();
    try {
      tokenIdentifier =
          new ContainerTokenIdentifier(containerId, containerVersion,
              nodeId.toString(), appSubmitter, capability, expiryTimeStamp,
              this.currentMasterKey.getMasterKey().getKeyId(),
              ResourceManager.getClusterTimeStamp(), priority, createTime,
              logAggregationContext, nodeLabelExpression, containerType,
              execType, allocationRequestId, allocationTags);
      password = this.createPassword(tokenIdentifier);

    } finally {
      this.readLock.unlock();
    }

    return BuilderUtils.newContainerToken(nodeId, password, tokenIdentifier);
  }
}