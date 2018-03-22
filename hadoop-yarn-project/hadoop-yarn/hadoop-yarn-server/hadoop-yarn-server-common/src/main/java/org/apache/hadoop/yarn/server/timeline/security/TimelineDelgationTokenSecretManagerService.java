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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

/**
 * Abstract implementation of delegation token manager service for different
 * versions of timeline service.
 */
public abstract class TimelineDelgationTokenSecretManagerService extends
    AbstractService {

  public TimelineDelgationTokenSecretManagerService(String name) {
    super(name);
  }

  private static long delegationTokenRemovalScanInterval = 3600000L;

  private AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier> secretManager = null;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    long secretKeyInterval =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL);
    long tokenMaxLifetime =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME);
    long tokenRenewInterval =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL);
    secretManager = createTimelineDelegationTokenSecretManager(
        secretKeyInterval, tokenMaxLifetime, tokenRenewInterval,
        delegationTokenRemovalScanInterval);
    super.init(conf);
  }

  protected abstract
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier>
          createTimelineDelegationTokenSecretManager(long secretKeyInterval,
          long tokenMaxLifetime, long tokenRenewInterval,
          long tokenRemovalScanInterval);

  @Override
  protected void serviceStart() throws Exception {
    secretManager.startThreads();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    secretManager.stopThreads();
    super.stop();
  }

  public AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier>
          getTimelineDelegationTokenSecretManager() {
    return secretManager;
  }
}
