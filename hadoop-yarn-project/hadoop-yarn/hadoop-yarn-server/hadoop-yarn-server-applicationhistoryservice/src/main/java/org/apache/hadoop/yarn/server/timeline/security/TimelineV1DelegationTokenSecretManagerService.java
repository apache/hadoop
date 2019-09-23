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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore.TimelineServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service wrapper of {@link TimelineV1DelegationTokenSecretManager}.
 */
@Private
@Unstable
public class TimelineV1DelegationTokenSecretManagerService extends
    TimelineDelgationTokenSecretManagerService {
  private TimelineStateStore stateStore = null;

  public TimelineV1DelegationTokenSecretManagerService() {
    super(TimelineV1DelegationTokenSecretManagerService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_RECOVERY_ENABLED)) {
      stateStore = createStateStore(conf);
      stateStore.init(conf);
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (stateStore != null) {
      stateStore.start();
      TimelineServiceState state = stateStore.loadState();
      ((TimelineV1DelegationTokenSecretManager)
          getTimelineDelegationTokenSecretManager()).recover(state);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stateStore != null) {
      stateStore.stop();
    }
    super.serviceStop();
  }

  @Override
  protected AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier>
      createTimelineDelegationTokenSecretManager(long secretKeyInterval,
          long tokenMaxLifetime, long tokenRenewInterval,
          long tokenRemovalScanInterval) {
    return new TimelineV1DelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, tokenRemovalScanInterval,
        stateStore);
  }

  protected TimelineStateStore createStateStore(
      Configuration conf) {
    return ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.TIMELINE_SERVICE_STATE_STORE_CLASS,
            LeveldbTimelineStateStore.class,
            TimelineStateStore.class), conf);
  }

  /**
   * Delegation token secret manager for ATSv1 and ATSv1.5.
   */
  @Private
  @Unstable
  public static class TimelineV1DelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    public static final Logger LOG =
        LoggerFactory.getLogger(TimelineV1DelegationTokenSecretManager.class);

    private TimelineStateStore stateStore;

    /**
     * Create a timeline v1 secret manager.
     * @param delegationKeyUpdateInterval the number of milliseconds for rolling
     *        new secret keys.
     * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
     *        tokens in milliseconds
     * @param delegationTokenRenewInterval how often the tokens must be renewed
     *        in milliseconds
     * @param delegationTokenRemoverScanInterval how often the tokens are
     *        scanned for expired tokens in milliseconds
     * @param stateStore timeline service state store
     */
    public TimelineV1DelegationTokenSecretManager(
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
      LOG.debug("Storing master key {}", key.getKeyId());
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
      LOG.debug("Removing master key {}", key.getKeyId());
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
      LOG.debug("Storing token {}", tokenId.getSequenceNumber());
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
      LOG.debug("Storing token {}", tokenId.getSequenceNumber());
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
      LOG.debug("Updating token {}", tokenId.getSequenceNumber());
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
