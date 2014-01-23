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
package org.apache.hadoop.lib.service.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.http.server.HttpFSServerWebApp;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.lib.server.BaseService;
import org.apache.hadoop.lib.server.ServerException;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.DelegationTokenIdentifier;
import org.apache.hadoop.lib.service.DelegationTokenManager;
import org.apache.hadoop.lib.service.DelegationTokenManagerException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * DelegationTokenManager service implementation.
 */
@InterfaceAudience.Private
public class DelegationTokenManagerService extends BaseService
  implements DelegationTokenManager {

  private static final String PREFIX = "delegation.token.manager";

  private static final String UPDATE_INTERVAL = "update.interval";

  private static final String MAX_LIFETIME = "max.lifetime";

  private static final String RENEW_INTERVAL = "renew.interval";

  private static final long HOUR = 60 * 60 * 1000;
  private static final long DAY = 24 * HOUR;

  DelegationTokenSecretManager secretManager = null;

  private Text tokenKind;

  public DelegationTokenManagerService() {
    super(PREFIX);
  }

  /**
   * Initializes the service.
   *
   * @throws ServiceException thrown if the service could not be initialized.
   */
  @Override
  protected void init() throws ServiceException {

    long updateInterval = getServiceConfig().getLong(UPDATE_INTERVAL, DAY);
    long maxLifetime = getServiceConfig().getLong(MAX_LIFETIME, 7 * DAY);
    long renewInterval = getServiceConfig().getLong(RENEW_INTERVAL, DAY);
    tokenKind = (HttpFSServerWebApp.get().isSslEnabled())
                ? SWebHdfsFileSystem.TOKEN_KIND : WebHdfsFileSystem.TOKEN_KIND;
    secretManager = new DelegationTokenSecretManager(tokenKind, updateInterval,
                                                     maxLifetime,
                                                     renewInterval, HOUR);
    try {
      secretManager.startThreads();
    } catch (IOException ex) {
      throw new ServiceException(ServiceException.ERROR.S12,
                                 DelegationTokenManager.class.getSimpleName(),
                                 ex.toString(), ex);
    }
  }

  /**
   * Destroys the service.
   */
  @Override
  public void destroy() {
    secretManager.stopThreads();
    super.destroy();
  }

  /**
   * Returns the service interface.
   *
   * @return the service interface.
   */
  @Override
  public Class getInterface() {
    return DelegationTokenManager.class;
  }

  /**
   * Creates a delegation token.
   *
   * @param ugi UGI creating the token.
   * @param renewer token renewer.
   * @return new delegation token.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * created.
   */
  @Override
  public Token<DelegationTokenIdentifier> createToken(UserGroupInformation ugi,
                                                      String renewer)
    throws DelegationTokenManagerException {
    renewer = (renewer == null) ? ugi.getShortUserName() : renewer;
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    DelegationTokenIdentifier tokenIdentifier =
      new DelegationTokenIdentifier(tokenKind, owner, new Text(renewer), realUser);
    Token<DelegationTokenIdentifier> token =
      new Token<DelegationTokenIdentifier>(tokenIdentifier, secretManager);
    try {
      SecurityUtil.setTokenService(token,
                                   HttpFSServerWebApp.get().getAuthority());
    } catch (ServerException ex) {
      throw new DelegationTokenManagerException(
        DelegationTokenManagerException.ERROR.DT04, ex.toString(), ex);
    }
    return token;
  }

  /**
   * Renews a delegation token.
   *
   * @param token delegation token to renew.
   * @param renewer token renewer.
   * @return epoc expiration time.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * renewed.
   */
  @Override
  public long renewToken(Token<DelegationTokenIdentifier> token, String renewer)
    throws DelegationTokenManagerException {
    try {
      return secretManager.renewToken(token, renewer);
    } catch (IOException ex) {
      throw new DelegationTokenManagerException(
        DelegationTokenManagerException.ERROR.DT02, ex.toString(), ex);
    }
  }

  /**
   * Cancels a delegation token.
   *
   * @param token delegation token to cancel.
   * @param canceler token canceler.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * canceled.
   */
  @Override
  public void cancelToken(Token<DelegationTokenIdentifier> token,
                          String canceler)
    throws DelegationTokenManagerException {
    try {
      secretManager.cancelToken(token, canceler);
    } catch (IOException ex) {
      throw new DelegationTokenManagerException(
        DelegationTokenManagerException.ERROR.DT03, ex.toString(), ex);
    }
  }

  /**
   * Verifies a delegation token.
   *
   * @param token delegation token to verify.
   * @return the UGI for the token.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * verified.
   */
  @Override
  public UserGroupInformation verifyToken(Token<DelegationTokenIdentifier> token)
    throws DelegationTokenManagerException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream dis = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier(tokenKind);
    try {
      id.readFields(dis);
      dis.close();
      secretManager.verifyToken(id, token.getPassword());
    } catch (Exception ex) {
      throw new DelegationTokenManagerException(
        DelegationTokenManagerException.ERROR.DT01, ex.toString(), ex);
    }
    return id.getUser();
  }

  private static class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

    private Text tokenKind;

    /**
     * Create a secret manager
     *
     * @param delegationKeyUpdateInterval the number of seconds for rolling new
     * secret keys.
     * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
     * tokens
     * @param delegationTokenRenewInterval how often the tokens must be renewed
     * @param delegationTokenRemoverScanInterval how often the tokens are
     * scanned
     * for expired tokens
     */
    public DelegationTokenSecretManager(Text tokenKind, long delegationKeyUpdateInterval,
                                        long delegationTokenMaxLifetime,
                                        long delegationTokenRenewInterval,
                                        long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
            delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
      this.tokenKind = tokenKind;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier(tokenKind);
    }

  }

}
