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

import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

import com.google.common.annotations.VisibleForTesting;

/**
 * AMRM-tokens are per ApplicationAttempt. If users redistribute their
 * tokens, it is their headache, god save them. I mean you are not supposed to
 * distribute keys to your vault, right? Anyways, ResourceManager saves each
 * token locally in memory till application finishes and to a store for restart,
 * so no need to remember master-keys even after rolling them.
 */
public class AMRMTokenSecretManager extends
    SecretManager<AMRMTokenIdentifier> {

  private static final Log LOG = LogFactory
    .getLog(AMRMTokenSecretManager.class);

  private int serialNo = new SecureRandom().nextInt();
  private MasterKeyData nextMasterKey;
  private MasterKeyData currentMasterKey;

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  private final Timer timer;
  private final long rollingInterval;
  private final long activationDelay;
  private RMContext rmContext;

  private final Set<ApplicationAttemptId> appAttemptSet =
      new HashSet<ApplicationAttemptId>();

  /**
   * Create an {@link AMRMTokenSecretManager}
   */
  public AMRMTokenSecretManager(Configuration conf, RMContext rmContext) {
    this.rmContext = rmContext;
    this.timer = new Timer();
    this.rollingInterval =
        conf
          .getLong(
            YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) * 1000;
    // Adding delay = 1.5 * expiry interval makes sure that all active AMs get
    // the updated shared-key.
    this.activationDelay =
        (long) (conf.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS) * 1.5);
    LOG.info("AMRMTokenKeyRollingInterval: " + this.rollingInterval
        + "ms and AMRMTokenKeyActivationDelay: " + this.activationDelay + " ms");
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS
              + " should be more than 2 X "
              + YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
    }
  }

  public void start() {
    if (this.currentMasterKey == null) {
      this.currentMasterKey = createNewMasterKey();
      AMRMTokenSecretManagerState state =
          AMRMTokenSecretManagerState.newInstance(
            this.currentMasterKey.getMasterKey(), null);
      rmContext.getStateStore().storeOrUpdateAMRMTokenSecretManagerState(state,
        false);
    }
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
      rollingInterval);
  }

  public void stop() {
    this.timer.cancel();
  }

  public void applicationMasterFinished(ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      LOG.info("Application finished, removing password for " + appAttemptId);
      this.appAttemptSet.remove(appAttemptId);
    } finally {
      this.writeLock.unlock();
    }
  }

  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }

  @Private
  void rollMasterKey() {
    this.writeLock.lock();
    try {
      LOG.info("Rolling master-key for amrm-tokens");
      this.nextMasterKey = createNewMasterKey();
      AMRMTokenSecretManagerState state =
          AMRMTokenSecretManagerState.newInstance(
            this.currentMasterKey.getMasterKey(),
            this.nextMasterKey.getMasterKey());
      rmContext.getStateStore().storeOrUpdateAMRMTokenSecretManagerState(state,
        true);
      this.timer.schedule(new NextKeyActivator(), this.activationDelay);
    } finally {
      this.writeLock.unlock();
    }
  }

  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      activateNextMasterKey();
    }
  }

  public void activateNextMasterKey() {
    this.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: "
          + this.nextMasterKey.getMasterKey().getKeyId());
      this.currentMasterKey = this.nextMasterKey;
      this.nextMasterKey = null;
      AMRMTokenSecretManagerState state =
          AMRMTokenSecretManagerState.newInstance(
            this.currentMasterKey.getMasterKey(), null);
      rmContext.getStateStore().storeOrUpdateAMRMTokenSecretManagerState(state,
        true);
    } finally {
      this.writeLock.unlock();
    }
  }

  @Private
  @VisibleForTesting
  public MasterKeyData createNewMasterKey() {
    this.writeLock.lock();
    try {
      return new MasterKeyData(serialNo++, generateSecret());
    } finally {
      this.writeLock.unlock();
    }
  }

  public Token<AMRMTokenIdentifier> createAndGetAMRMToken(
      ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      LOG.info("Create AMRMToken for ApplicationAttempt: " + appAttemptId);
      AMRMTokenIdentifier identifier =
          new AMRMTokenIdentifier(appAttemptId, getMasterKey().getMasterKey()
            .getKeyId());
      byte[] password = this.createPassword(identifier);
      appAttemptSet.add(appAttemptId);
      return new Token<AMRMTokenIdentifier>(identifier.getBytes(), password,
        identifier.getKind(), new Text());
    } finally {
      this.writeLock.unlock();
    }
  }

  // If nextMasterKey is not Null, then return nextMasterKey
  // otherwise return currentMasterKey
  @VisibleForTesting
  public MasterKeyData getMasterKey() {
    this.readLock.lock();
    try {
      return nextMasterKey == null ? currentMasterKey : nextMasterKey;
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Populate persisted password of AMRMToken back to AMRMTokenSecretManager.
   */
  public void addPersistedPassword(Token<AMRMTokenIdentifier> token)
      throws IOException {
    this.writeLock.lock();
    try {
      AMRMTokenIdentifier identifier = token.decodeIdentifier();
      LOG.debug("Adding password for " + identifier.getApplicationAttemptId());
      appAttemptSet.add(identifier.getApplicationAttemptId());
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * Retrieve the password for the given {@link AMRMTokenIdentifier}.
   * Used by RPC layer to validate a remote {@link AMRMTokenIdentifier}.
   */
  @Override
  public byte[] retrievePassword(AMRMTokenIdentifier identifier)
      throws InvalidToken {
    this.readLock.lock();
    try {
      ApplicationAttemptId applicationAttemptId =
          identifier.getApplicationAttemptId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to retrieve password for " + applicationAttemptId);
      }
      if (!appAttemptSet.contains(applicationAttemptId)) {
        throw new InvalidToken(applicationAttemptId
            + " not found in AMRMTokenSecretManager.");
      }
      if (identifier.getKeyId() == this.currentMasterKey.getMasterKey()
        .getKeyId()) {
        return createPassword(identifier.getBytes(),
          this.currentMasterKey.getSecretKey());
      } else if (nextMasterKey != null
          && identifier.getKeyId() == this.nextMasterKey.getMasterKey()
            .getKeyId()) {
        return createPassword(identifier.getBytes(),
          this.nextMasterKey.getSecretKey());
      }
      throw new InvalidToken("Invalid AMRMToken from " + applicationAttemptId);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Creates an empty TokenId to be used for de-serializing an
   * {@link AMRMTokenIdentifier} by the RPC layer.
   */
  @Override
  public AMRMTokenIdentifier createIdentifier() {
    return new AMRMTokenIdentifier();
  }

  @Private
  @VisibleForTesting
  public MasterKeyData getCurrnetMasterKeyData() {
    this.readLock.lock();
    try {
      return this.currentMasterKey;
    } finally {
      this.readLock.unlock();
    }
  }

  @Private
  @VisibleForTesting
  public MasterKeyData getNextMasterKeyData() {
    this.readLock.lock();
    try {
      return this.nextMasterKey;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  @Private
  protected byte[] createPassword(AMRMTokenIdentifier identifier) {
    this.readLock.lock();
    try {
      ApplicationAttemptId applicationAttemptId =
          identifier.getApplicationAttemptId();
      LOG.info("Creating password for " + applicationAttemptId);
      return createPassword(identifier.getBytes(), getMasterKey()
        .getSecretKey());
    } finally {
      this.readLock.unlock();
    }
  }

  public void recover(RMState state) {
    if (state.getAMRMTokenSecretManagerState() != null) {
      // recover the current master key
      MasterKey currentKey =
          state.getAMRMTokenSecretManagerState().getCurrentMasterKey();
      this.currentMasterKey =
          new MasterKeyData(currentKey, createSecretKey(currentKey.getBytes()
            .array()));

      // recover the next master key if not null
      MasterKey nextKey =
          state.getAMRMTokenSecretManagerState().getNextMasterKey();
      if (nextKey != null) {
        this.nextMasterKey =
            new MasterKeyData(nextKey, createSecretKey(nextKey.getBytes()
              .array()));
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    }
  }
}
