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

import java.security.SecureRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecretManager for ContainerTokens. Extended by both RM and NM and hence is
 * present in yarn-server-common package.
 * 
 */
public class BaseContainerTokenSecretManager extends
    SecretManager<ContainerTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseContainerTokenSecretManager.class);

  protected int serialNo = new SecureRandom().nextInt();

  protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  protected final Lock readLock = readWriteLock.readLock();
  protected final Lock writeLock = readWriteLock.writeLock();

  /**
   * THE masterKey. ResourceManager should persist this and recover it on
   * restart instead of generating a new key. The NodeManagers get it from the
   * ResourceManager and use it for validating container-tokens.
   */
  protected MasterKeyData currentMasterKey;

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
      return new MasterKeyData(serialNo++, generateSecret());
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
    LOG.debug("Creating password for {} for user {} to be run on NM {}",
        identifier.getContainerID(), identifier.getUser(),
        identifier.getNmHostAddress());
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
    LOG.debug("Retrieving password for {} for user {} to be run on NM {}",
        identifier.getContainerID(), identifier.getUser(),
        identifier.getNmHostAddress());
    return createPassword(identifier.getBytes(), masterKey.getSecretKey());
  }

  /**
   * Used by the RPC layer.
   */
  @Override
  public ContainerTokenIdentifier createIdentifier() {
    return new ContainerTokenIdentifier();
  }
}
