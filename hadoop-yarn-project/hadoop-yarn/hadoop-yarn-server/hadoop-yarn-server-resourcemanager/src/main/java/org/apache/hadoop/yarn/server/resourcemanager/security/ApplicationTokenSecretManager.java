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

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;

/**
 * Application-tokens are per ApplicationAttempt. If users redistribute their
 * tokens, it is their headache, god save them. I mean you are not supposed to
 * distribute keys to your vault, right? Anyways, ResourceManager saves each
 * token locally in memory till application finishes and to a store for restart,
 * so no need to remember master-keys even after rolling them.
 */
public class ApplicationTokenSecretManager extends
    SecretManager<ApplicationTokenIdentifier> {

  private static final Log LOG = LogFactory
    .getLog(ApplicationTokenSecretManager.class);

  private SecretKey masterKey;
  private final Timer timer;
  private final long rollingInterval;

  private final Map<ApplicationAttemptId, byte[]> passwords =
      new HashMap<ApplicationAttemptId, byte[]>();

  /**
   * Create an {@link ApplicationTokenSecretManager}
   */
  public ApplicationTokenSecretManager(Configuration conf) {
    rollMasterKey();
    this.timer = new Timer();
    this.rollingInterval =
        conf
          .getLong(
            YarnConfiguration.RM_APP_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_RM_APP_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) * 1000;
  }

  public void start() {
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), 0, rollingInterval);
  }

  public void stop() {
    this.timer.cancel();
  }

  public synchronized void applicationMasterFinished(
      ApplicationAttemptId appAttemptId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Application finished, removing password for " + appAttemptId);
    }
    this.passwords.remove(appAttemptId);
  }

  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }

  @Private
  public synchronized void setMasterKey(SecretKey masterKey) {
    this.masterKey = masterKey;
  }

  @Private
  public synchronized SecretKey getMasterKey() {
    return this.masterKey;
  }

  @Private
  synchronized void rollMasterKey() {
    LOG.info("Rolling master-key for application-tokens");
    this.masterKey = generateSecret();
  }

  /**
   * Create a password for a given {@link ApplicationTokenIdentifier}. Used to
   * send to the AppicationAttempt which can give it back during authentication.
   */
  @Override
  public synchronized byte[] createPassword(
      ApplicationTokenIdentifier identifier) {
    ApplicationAttemptId applicationAttemptId =
        identifier.getApplicationAttemptId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating password for " + applicationAttemptId);
    }
    byte[] password = createPassword(identifier.getBytes(), masterKey);
    this.passwords.put(applicationAttemptId, password);
    return password;
  }

  /**
   * Retrieve the password for the given {@link ApplicationTokenIdentifier}.
   * Used by RPC layer to validate a remote {@link ApplicationTokenIdentifier}.
   */
  @Override
  public synchronized byte[] retrievePassword(
      ApplicationTokenIdentifier identifier) throws InvalidToken {
    ApplicationAttemptId applicationAttemptId =
        identifier.getApplicationAttemptId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to retrieve password for " + applicationAttemptId);
    }
    byte[] password = this.passwords.get(applicationAttemptId);
    if (password == null) {
      throw new InvalidToken("Password not found for ApplicationAttempt "
          + applicationAttemptId);
    }
    return password;
  }

  /**
   * Creates an empty TokenId to be used for de-serializing an
   * {@link ApplicationTokenIdentifier} by the RPC layer.
   */
  @Override
  public ApplicationTokenIdentifier createIdentifier() {
    return new ApplicationTokenIdentifier();
  }

}
