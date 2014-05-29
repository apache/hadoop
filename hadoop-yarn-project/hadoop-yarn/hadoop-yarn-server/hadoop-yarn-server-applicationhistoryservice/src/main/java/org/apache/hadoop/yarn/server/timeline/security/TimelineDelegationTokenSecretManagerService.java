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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * The service wrapper of {@link TimelineDelegationTokenSecretManager}
 */
@Private
@Unstable
public class TimelineDelegationTokenSecretManagerService extends AbstractService {

  private TimelineDelegationTokenSecretManager secretManager = null;
  private InetSocketAddress serviceAddr = null;

  public TimelineDelegationTokenSecretManagerService() {
    super(TimelineDelegationTokenSecretManagerService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    long secretKeyInterval =
        conf.getLong(YarnConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_KEY,
            YarnConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
        conf.getLong(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
        conf.getLong(YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    secretManager = new TimelineDelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval,
        3600000);
    secretManager.startThreads();

    serviceAddr = TimelineUtils.getTimelineTokenServiceAddress(getConfig());
    super.init(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    secretManager.stopThreads();
    super.stop();
  }

  /**
   * Creates a delegation token.
   *
   * @param ugi UGI creating the token.
   * @param renewer token renewer.
   * @return new delegation token.
   * @throws IOException thrown if the token could not be created.
   */
  public Token<TimelineDelegationTokenIdentifier> createToken(
      UserGroupInformation ugi, String renewer) throws IOException {
    renewer = (renewer == null) ? ugi.getShortUserName() : renewer;
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    TimelineDelegationTokenIdentifier tokenIdentifier =
        new TimelineDelegationTokenIdentifier(owner, new Text(renewer), realUser);
    Token<TimelineDelegationTokenIdentifier> token =
        new Token<TimelineDelegationTokenIdentifier>(tokenIdentifier, secretManager);
    SecurityUtil.setTokenService(token, serviceAddr);
    return token;
  }

  /**
   * Renews a delegation token.
   *
   * @param token delegation token to renew.
   * @param renewer token renewer.
   * @throws IOException thrown if the token could not be renewed.
   */
  public long renewToken(Token<TimelineDelegationTokenIdentifier> token,
      String renewer) throws IOException {
      return secretManager.renewToken(token, renewer);
  }

  /**
   * Cancels a delegation token.
   *
   * @param token delegation token to cancel.
   * @param canceler token canceler.
   * @throws IOException thrown if the token could not be canceled.
   */
  public void cancelToken(Token<TimelineDelegationTokenIdentifier> token,
      String canceler) throws IOException {
    secretManager.cancelToken(token, canceler);
  }

  /**
   * Verifies a delegation token.
   *
   * @param token delegation token to verify.
   * @return the UGI for the token.
   * @throws IOException thrown if the token could not be verified.
   */
  public UserGroupInformation verifyToken(Token<TimelineDelegationTokenIdentifier> token)
    throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream dis = new DataInputStream(buf);
    TimelineDelegationTokenIdentifier id = new TimelineDelegationTokenIdentifier();
    try {
      id.readFields(dis);
      secretManager.verifyToken(id, token.getPassword());
    } finally {
      dis.close();
    }
    return id.getUser();
  }

  /**
   * Create a timeline secret manager
   * 
   * @param delegationKeyUpdateInterval
   *          the number of seconds for rolling new secret keys.
   * @param delegationTokenMaxLifetime
   *          the maximum lifetime of the delegation tokens
   * @param delegationTokenRenewInterval
   *          how often the tokens must be renewed
   * @param delegationTokenRemoverScanInterval
   *          how often the tokens are scanned for expired tokens
   */
  @Private
  @Unstable
  public static class TimelineDelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    public TimelineDelegationTokenSecretManager(long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    @Override
    public TimelineDelegationTokenIdentifier createIdentifier() {
      return new TimelineDelegationTokenIdentifier();
    }

  }

}
