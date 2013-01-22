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

package org.apache.hadoop.yarn.server.security;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * SecretManager for ContainerTokens. Extended by both RM and NM and hence is
 * present in yarn-server-common package.
 * 
 */
public class BaseContainerTokenSecretManager extends
    SecretManager<ContainerTokenIdentifier> {

  private static Log LOG = LogFactory
    .getLog(BaseContainerTokenSecretManager.class);

  private int serialNo = new SecureRandom().nextInt();

  protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  protected final Lock readLock = readWriteLock.readLock();
  protected final Lock writeLock = readWriteLock.writeLock();

  /**
   * THE masterKey. ResourceManager should persist this and recover it on
   * restart instead of generating a new key. The NodeManagers get it from the
   * ResourceManager and use it for validating container-tokens.
   */
  protected MasterKeyData currentMasterKey;

  protected final class MasterKeyData {

    private final MasterKey masterKeyRecord;
    // Underlying secret-key also stored to avoid repetitive encoding and
    // decoding the masterKeyRecord bytes.
    private final SecretKey generatedSecretKey;

    private MasterKeyData() {
      this.masterKeyRecord = Records.newRecord(MasterKey.class);
      this.masterKeyRecord.setKeyId(serialNo++);
      this.generatedSecretKey = generateSecret();
      this.masterKeyRecord.setBytes(ByteBuffer.wrap(generatedSecretKey
        .getEncoded()));
    }

    public MasterKeyData(MasterKey masterKeyRecord) {
      this.masterKeyRecord = masterKeyRecord;
      this.generatedSecretKey =
          SecretManager.createSecretKey(this.masterKeyRecord.getBytes().array()
            .clone());
    }

    public MasterKey getMasterKey() {
      return this.masterKeyRecord;
    }

    private SecretKey getSecretKey() {
      return this.generatedSecretKey;
    }
  }

  protected final long containerTokenExpiryInterval;

  public BaseContainerTokenSecretManager(Configuration conf) {
    this.containerTokenExpiryInterval =
        conf.getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
  }

  // Need lock as we increment serialNo etc.
  protected MasterKeyData createNewMasterKey() {
    this.writeLock.lock();
    try {
    return new MasterKeyData();
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Private
  public MasterKey getCurrentKey() {
    this.readLock.lock();
    try {
    return this.currentMasterKey.getMasterKey();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public byte[] createPassword(ContainerTokenIdentifier identifier) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating password for " + identifier.getContainerID()
          + " for user " + identifier.getUser() + " to be run on NM "
          + identifier.getNmHostAddress());
    }
    this.readLock.lock();
    try {
      return createPassword(identifier.getBytes(),
        this.currentMasterKey.getSecretKey());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public byte[] retrievePassword(ContainerTokenIdentifier identifier)
      throws SecretManager.InvalidToken {
    this.readLock.lock();
    try {
      return retrievePasswordInternal(identifier, this.currentMasterKey);
    } finally {
      this.readLock.unlock();
    }
  }

  protected byte[] retrievePasswordInternal(ContainerTokenIdentifier identifier,
      MasterKeyData masterKey)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Retrieving password for " + identifier.getContainerID()
          + " for user " + identifier.getUser() + " to be run on NM "
          + identifier.getNmHostAddress());
    }
    return createPassword(identifier.getBytes(), masterKey.getSecretKey());
  }

  /**
   * Used by the RPC layer.
   */
  @Override
  public ContainerTokenIdentifier createIdentifier() {
    return new ContainerTokenIdentifier();
  }

  /**
   * Helper function for creating ContainerTokens
   * 
   * @param containerId
   * @param nodeId
   * @param appSubmitter
   * @param capability
   * @return the container-token
   */
  public ContainerToken createContainerToken(ContainerId containerId,
      NodeId nodeId, String appSubmitter, Resource capability) {
    byte[] password;
    ContainerTokenIdentifier tokenIdentifier;
    long expiryTimeStamp =
        System.currentTimeMillis() + containerTokenExpiryInterval;

    // Lock so that we use the same MasterKey's keyId and its bytes
    this.readLock.lock();
    try {
      tokenIdentifier =
          new ContainerTokenIdentifier(containerId, nodeId.toString(),
            appSubmitter, capability, expiryTimeStamp, this.currentMasterKey
              .getMasterKey().getKeyId());
      password = this.createPassword(tokenIdentifier);

    } finally {
      this.readLock.unlock();
    }

    return BuilderUtils.newContainerToken(nodeId, password, tokenIdentifier);
  }
}
