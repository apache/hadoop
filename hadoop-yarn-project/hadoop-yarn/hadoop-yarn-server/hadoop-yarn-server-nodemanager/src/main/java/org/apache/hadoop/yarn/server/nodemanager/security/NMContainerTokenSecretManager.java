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

package org.apache.hadoop.yarn.server.nodemanager.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerTokensState;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

/**
 * The NM maintains only two master-keys. The current key that RM knows and the
 * key from the previous rolling-interval.
 * 
 */
public class NMContainerTokenSecretManager extends
    BaseContainerTokenSecretManager {

  private static final Log LOG = LogFactory
      .getLog(NMContainerTokenSecretManager.class);
  
  private MasterKeyData previousMasterKey;
  private final TreeMap<Long, List<ContainerId>> recentlyStartedContainerTracker;
  private final NMStateStoreService stateStore;
  
  private String nodeHostAddr;
  
  public NMContainerTokenSecretManager(Configuration conf) {
    this(conf, new NMNullStateStoreService());
  }

  public NMContainerTokenSecretManager(Configuration conf,
      NMStateStoreService stateStore) {
    super(conf);
    recentlyStartedContainerTracker =
        new TreeMap<Long, List<ContainerId>>();
    this.stateStore = stateStore;
  }

  public synchronized void recover()
      throws IOException {
    RecoveredContainerTokensState state =
        stateStore.loadContainerTokensState();
    MasterKey key = state.getCurrentMasterKey();
    if (key != null) {
      super.currentMasterKey =
          new MasterKeyData(key, createSecretKey(key.getBytes().array()));
    }

    key = state.getPreviousMasterKey();
    if (key != null) {
      previousMasterKey =
          new MasterKeyData(key, createSecretKey(key.getBytes().array()));
    }

    // restore the serial number from the current master key
    if (super.currentMasterKey != null) {
      super.serialNo = super.currentMasterKey.getMasterKey().getKeyId() + 1;
    }

    for (Entry<ContainerId, Long> entry : state.getActiveTokens().entrySet()) {
      ContainerId containerId = entry.getKey();
      Long expTime = entry.getValue();
      List<ContainerId> containerList =
          recentlyStartedContainerTracker.get(expTime);
      if (containerList == null) {
        containerList = new ArrayList<ContainerId>();
        recentlyStartedContainerTracker.put(expTime, containerList);
      }
      if (!containerList.contains(containerId)) {
        containerList.add(containerId);
      }
    }
  }

  private void updateCurrentMasterKey(MasterKeyData key) {
    super.currentMasterKey = key;
    try {
      stateStore.storeContainerTokenCurrentMasterKey(key.getMasterKey());
    } catch (IOException e) {
      LOG.error("Unable to update current master key in state store", e);
    }
  }

  private void updatePreviousMasterKey(MasterKeyData key) {
    previousMasterKey = key;
    try {
      stateStore.storeContainerTokenPreviousMasterKey(key.getMasterKey());
    } catch (IOException e) {
      LOG.error("Unable to update previous master key in state store", e);
    }
  }

  /**
   * Used by NodeManagers to create a token-secret-manager with the key obtained
   * from the RM. This can happen during registration or when the RM rolls the
   * master-key and signals the NM.
   * 
   * @param masterKeyRecord
   */
  @Private
  public synchronized void setMasterKey(MasterKey masterKeyRecord) {
    // Update keys only if the key has changed.
    if (super.currentMasterKey == null || super.currentMasterKey.getMasterKey()
          .getKeyId() != masterKeyRecord.getKeyId()) {
      LOG.info("Rolling master-key for container-tokens, got key with id "
          + masterKeyRecord.getKeyId());
      if (super.currentMasterKey != null) {
        updatePreviousMasterKey(super.currentMasterKey);
      }
      updateCurrentMasterKey(new MasterKeyData(masterKeyRecord,
          createSecretKey(masterKeyRecord.getBytes().array())));
    }
  }

  /**
   * Override of this is to validate ContainerTokens generated by using
   * different {@link MasterKey}s.
   */
  @Override
  public synchronized byte[] retrievePassword(
      ContainerTokenIdentifier identifier) throws SecretManager.InvalidToken {
    int keyId = identifier.getMasterKeyId();

    MasterKeyData masterKeyToUse = null;
    if (this.previousMasterKey != null
        && keyId == this.previousMasterKey.getMasterKey().getKeyId()) {
      // A container-launch has come in with a token generated off the last
      // master-key
      masterKeyToUse = this.previousMasterKey;
    } else if (keyId == super.currentMasterKey.getMasterKey().getKeyId()) {
      // A container-launch has come in with a token generated off the current
      // master-key
      masterKeyToUse = super.currentMasterKey;
    }

    if (nodeHostAddr != null
        && !identifier.getNmHostAddress().equals(nodeHostAddr)) {
      // Valid container token used for incorrect node.
      throw new SecretManager.InvalidToken("Given Container "
          + identifier.getContainerID().toString()
          + " identifier is not valid for current Node manager. Expected : "
          + nodeHostAddr + " Found : " + identifier.getNmHostAddress());
    }
    
    if (masterKeyToUse != null) {
      return retrievePasswordInternal(identifier, masterKeyToUse);
    }

    // Invalid request. Like startContainer() with token generated off
    // old-master-keys.
    throw new SecretManager.InvalidToken("Given Container "
        + identifier.getContainerID().toString()
        + " seems to have an illegally generated token.");
  }

  /**
   * Container start has gone through. We need to store the containerId in order
   * to block future container start requests with same container token. This
   * container token needs to be saved till its container token expires.
   */
  public synchronized void startContainerSuccessful(
      ContainerTokenIdentifier tokenId) {

    removeAnyContainerTokenIfExpired();
    
    ContainerId containerId = tokenId.getContainerID();
    Long expTime = tokenId.getExpiryTimeStamp();
    // We might have multiple containers with same expiration time.
    if (!recentlyStartedContainerTracker.containsKey(expTime)) {
      recentlyStartedContainerTracker
        .put(expTime, new ArrayList<ContainerId>());
    }
    recentlyStartedContainerTracker.get(expTime).add(containerId);
    try {
      stateStore.storeContainerToken(containerId, expTime);
    } catch (IOException e) {
      LOG.error("Unable to store token for container " + containerId, e);
    }
  }

  protected synchronized void removeAnyContainerTokenIfExpired() {
    // Trying to remove any container if its container token has expired.
    Iterator<Entry<Long, List<ContainerId>>> containersI =
        this.recentlyStartedContainerTracker.entrySet().iterator();
    Long currTime = System.currentTimeMillis();
    while (containersI.hasNext()) {
      Entry<Long, List<ContainerId>> containerEntry = containersI.next();
      if (containerEntry.getKey() < currTime) {
        for (ContainerId container : containerEntry.getValue()) {
          try {
            stateStore.removeContainerToken(container);
          } catch (IOException e) {
            LOG.error("Unable to remove token for container " + container, e);
          }
        }
        containersI.remove();
      } else {
        break;
      }
    }
  }

  /**
   * Container will be remembered based on expiration time of the container
   * token used for starting the container. It is safe to use expiration time
   * as there is one to many mapping between expiration time and containerId.
   * @return true if the current token identifier is not present in cache.
   */
  public synchronized boolean isValidStartContainerRequest(
      ContainerTokenIdentifier containerTokenIdentifier) {

    removeAnyContainerTokenIfExpired();

    Long expTime = containerTokenIdentifier.getExpiryTimeStamp();
    List<ContainerId> containers =
        this.recentlyStartedContainerTracker.get(expTime);
    if (containers == null
        || !containers.contains(containerTokenIdentifier.getContainerID())) {
      return true;
    } else {
      return false;
    }
  }

  public synchronized void setNodeId(NodeId nodeId) {
    nodeHostAddr = nodeId.toString();
    LOG.info("Updating node address : " + nodeHostAddr);
  }
}