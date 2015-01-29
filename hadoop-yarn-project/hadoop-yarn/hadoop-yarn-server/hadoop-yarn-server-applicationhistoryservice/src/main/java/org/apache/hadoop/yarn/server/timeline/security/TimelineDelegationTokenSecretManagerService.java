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

package org.apache.hadoop.yarn.server.timeline.security;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore.TimelineServiceState;

/**
 * The service wrapper of {@link TimelineDelegationTokenSecretManager}
 */
@Private
@Unstable
public class TimelineDelegationTokenSecretManagerService extends
    AbstractService {

  private TimelineDelegationTokenSecretManager secretManager = null;
  private TimelineStateStore stateStore = null;

  public TimelineDelegationTokenSecretManagerService() {
    super(TimelineDelegationTokenSecretManagerService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_RECOVERY_ENABLED)) {
      stateStore = createStateStore(conf);
      stateStore.init(conf);
    }

    long secretKeyInterval =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL);
    long tokenMaxLifetime =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME);
    long tokenRenewInterval =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL);
    secretManager = new TimelineDelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, 3600000, stateStore);
    super.init(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (stateStore != null) {
      stateStore.start();
      TimelineServiceState state = stateStore.loadState();
      secretManager.recover(state);
    }

    secretManager.startThreads();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stateStore != null) {
      stateStore.stop();
    }

    secretManager.stopThreads();
    super.stop();
  }

  protected TimelineStateStore createStateStore(
      Configuration conf) {
    return ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.TIMELINE_SERVICE_STATE_STORE_CLASS,
            LeveldbTimelineStateStore.class,
            TimelineStateStore.class), conf);
  }

  /**
   * Ge the instance of {link #TimelineDelegationTokenSecretManager}
   *
   * @return the instance of {link #TimelineDelegationTokenSecretManager}
   */
  public TimelineDelegationTokenSecretManager
  getTimelineDelegationTokenSecretManager() {
    return secretManager;
  }

  @Private
  @Unstable
  public static class TimelineDelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    public static final Log LOG =
        LogFactory.getLog(TimelineDelegationTokenSecretManager.class);

    private TimelineStateStore stateStore;

    /**
     * Create a timeline secret manager
     *
     * @param delegationKeyUpdateInterval the number of seconds for rolling new secret keys.
     * @param delegationTokenMaxLifetime the maximum lifetime of the delegation tokens
     * @param delegationTokenRenewInterval how often the tokens must be renewed
     * @param delegationTokenRemoverScanInterval how often the tokens are scanned for expired tokens
     */
    public TimelineDelegationTokenSecretManager(
        long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime,
        long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval,
        TimelineStateStore stateStore) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
      this.stateStore = stateStore;
    }

    @Override
    public TimelineDelegationTokenIdentifier createIdentifier() {
      return new TimelineDelegationTokenIdentifier();
    }

    @Override
    protected void storeNewMasterKey(DelegationKey key) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing master key " + key.getKeyId());
      }
      try {
        if (stateStore != null) {
          stateStore.storeTokenMasterKey(key);
        }
      } catch (IOException e) {
        LOG.error("Unable to store master key " + key.getKeyId(), e);
      }
    }

    @Override
    protected void removeStoredMasterKey(DelegationKey key) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing master key " + key.getKeyId());
      }
      try {
        if (stateStore != null) {
          stateStore.removeTokenMasterKey(key);
        }
      } catch (IOException e) {
        LOG.error("Unable to remove master key " + key.getKeyId(), e);
      }
    }

    @Override
    protected void storeNewToken(TimelineDelegationTokenIdentifier tokenId,
        long renewDate) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing token " + tokenId.getSequenceNumber());
      }
      try {
        if (stateStore != null) {
          stateStore.storeToken(tokenId, renewDate);
        }
      } catch (IOException e) {
        LOG.error("Unable to store token " + tokenId.getSequenceNumber(), e);
      }
    }

    @Override
    protected void removeStoredToken(TimelineDelegationTokenIdentifier tokenId)
        throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing token " + tokenId.getSequenceNumber());
      }
      try {
        if (stateStore != null) {
          stateStore.removeToken(tokenId);
        }
      } catch (IOException e) {
        LOG.error("Unable to remove token " + tokenId.getSequenceNumber(), e);
      }
    }

    @Override
    protected void updateStoredToken(TimelineDelegationTokenIdentifier tokenId,
        long renewDate) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating token " + tokenId.getSequenceNumber());
      }
      try {
        if (stateStore != null) {
          stateStore.updateToken(tokenId, renewDate);
        }
      } catch (IOException e) {
        LOG.error("Unable to update token " + tokenId.getSequenceNumber(), e);
      }
    }

    public void recover(TimelineServiceState state) throws IOException {
      LOG.info("Recovering " + getClass().getSimpleName());
      for (DelegationKey key : state.getTokenMasterKeyState()) {
        addKey(key);
      }
      this.delegationTokenSequenceNumber = state.getLatestSequenceNumber();
      for (Entry<TimelineDelegationTokenIdentifier, Long> entry :
          state.getTokenState().entrySet()) {
        addPersistedDelegationToken(entry.getKey(), entry.getValue());
      }
    }
  }

}
