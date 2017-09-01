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

package org.apache.hadoop.yarn.server.timelineservice.security;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.timeline.security.TimelineDelgationTokenSecretManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service wrapper of {@link TimelineV2DelegationTokenSecretManager}.
 */
public class TimelineV2DelegationTokenSecretManagerService extends
    TimelineDelgationTokenSecretManagerService {

  public TimelineV2DelegationTokenSecretManagerService() {
    super(TimelineV2DelegationTokenSecretManagerService.class.getName());
  }

  @Override
  protected AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier>
      createTimelineDelegationTokenSecretManager(long secretKeyInterval,
          long tokenMaxLifetime, long tokenRenewInterval,
          long tokenRemovalScanInterval) {
    return new TimelineV2DelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, tokenRemovalScanInterval);
  }

  public Token<TimelineDelegationTokenIdentifier> generateToken(
      UserGroupInformation ugi, String renewer) {
    return ((TimelineV2DelegationTokenSecretManager)
        getTimelineDelegationTokenSecretManager()).generateToken(ugi, renewer);
  }

  public long renewToken(Token<TimelineDelegationTokenIdentifier> token,
      String renewer) throws IOException {
    return getTimelineDelegationTokenSecretManager().renewToken(token, renewer);
  }

  public void cancelToken(Token<TimelineDelegationTokenIdentifier> token,
      String canceller) throws IOException {
    getTimelineDelegationTokenSecretManager().cancelToken(token, canceller);
  }

  /**
   * Delegation token secret manager for ATSv2.
   */
  @Private
  @Unstable
  public static class TimelineV2DelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    private static final Logger LOG =
        LoggerFactory.getLogger(TimelineV2DelegationTokenSecretManager.class);

    /**
     * Create a timeline v2 secret manager.
     * @param delegationKeyUpdateInterval the number of milliseconds for rolling
     *        new secret keys.
     * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
     *        tokens in milliseconds
     * @param delegationTokenRenewInterval how often the tokens must be renewed
     *        in milliseconds
     * @param delegationTokenRemoverScanInterval how often the tokens are
     *        scanned for expired tokens in milliseconds
     */
    public TimelineV2DelegationTokenSecretManager(
        long delegationKeyUpdateInterval, long delegationTokenMaxLifetime,
        long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    public Token<TimelineDelegationTokenIdentifier> generateToken(
        UserGroupInformation ugi, String renewer) {
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      TimelineDelegationTokenIdentifier identifier = createIdentifier();
      identifier.setOwner(new Text(ugi.getUserName()));
      identifier.setRenewer(new Text(renewer));
      identifier.setRealUser(realUser);
      byte[] password = createPassword(identifier);
      return new Token<TimelineDelegationTokenIdentifier>(identifier.getBytes(),
          password, identifier.getKind(), null);
    }

    @Override
    public TimelineDelegationTokenIdentifier createIdentifier() {
      return new TimelineDelegationTokenIdentifier();
    }

    @Override
    protected void logExpireToken(TimelineDelegationTokenIdentifier ident)
        throws IOException {
      LOG.info("Token " + ident + " expired.");
    }
  }
}
