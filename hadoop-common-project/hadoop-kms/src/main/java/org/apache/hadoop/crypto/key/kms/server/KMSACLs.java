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
package org.apache.hadoop.crypto.key.kms.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyACLs;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base skeleton class for the management of the Keys.
 * Used by the KMSWebApp to communicate with the key backend storage.
 */
@InterfaceAudience.Private
public abstract class KMSACLs implements Runnable, KeyACLs {
  private static final Logger LOG = LoggerFactory.getLogger(KMSACLs.class);

  private static final String UNAUTHORIZED_MSG_WITH_KEY =
      "User:%s not allowed to do '%s' on '%s'";

  private static final String UNAUTHORIZED_MSG_WITHOUT_KEY =
      "User:%s not allowed to do '%s'";

  private static final int RELOADER_SLEEP_MILLIS = 1000;

  private ScheduledExecutorService executorService;

  /**
   * Enumeration of all the different key operation type.
   *
   */
  public enum Type {
    CREATE, DELETE, ROLLOVER, GET, GET_KEYS, GET_METADATA,
    SET_KEY_MATERIAL, GENERATE_EEK, DECRYPT_EEK;

    public String getAclConfigKey() {
      return KMSConfiguration.CONFIG_PREFIX + "acl." + this.toString();
    }

    public String getBlacklistConfigKey() {
      return KMSConfiguration.CONFIG_PREFIX + "blacklist." + this.toString();
    }
  }

  /**
   * First Check if user is in ACL for the KMS operation, if yes, then return
   * true if user is not present in any configured blacklist for the operation.
   *
   * @param keyOperationType
   *          KMS Operation
   * @param ugi
   *          UserGroupInformation of user
   * @return true is user has access
   */
  public abstract boolean hasAccess(Type keyOperationType,
      UserGroupInformation ugi);

  /**
   * This is called by the KeyProvider to check if the given user is
   * authorized to perform the specified operation on the given acl name.
   * @param aclName name of the key ACL
   * @param ugi User's UserGroupInformation
   * @param opType Operation Type
   * @return true if user has access to the aclName and opType else false
   */
  @Override
  public abstract boolean hasAccessToKey(String aclName,
      UserGroupInformation ugi, KeyOpType opType);

  /**
   *
   * @param aclName ACL name
   * @param opType Operation Type
   * @return true if AclName exists else false
   */
  @Override
  public abstract boolean isACLPresent(String aclName, KeyOpType opType);

  /**
   * Loads the ACLs from the persistent store.
   *
   * @param forceReload
   *          if true, the cache should be avoided.
   */
  public abstract void loadAcls(boolean forceReload);

  /**
  * Starts the reloader background process.
  */
  public synchronized void startReloader() {
    if (executorService == null) {
      LOG.debug("Starting background reloader for ACL list, implementation: "
          + this.getClass().getName());
      executorService = Executors.newScheduledThreadPool(1);
      executorService.scheduleAtFixedRate(this, RELOADER_SLEEP_MILLIS,
          RELOADER_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Stops the reloader background process.
   */
  public synchronized void stopReloader() {
    if (executorService != null) {
      LOG.debug("Stoping background reloader for ACL list");
      executorService.shutdownNow();
      executorService = null;
    }
  }

  @Override
  public void run() {
    loadAcls(false);
  }

  /**
   * Asserts, that the given user has the necessary rights to perform the given
   * operation on the key.
   *
   * @param operationType
   *          the Key operation type.
   * @param ugi
   *          UserGroupInformation of the user
   * @param operation
   *          the KMS Operation
   * @param key
   *          the key name
   * @throws AccessControlException
   *           if the access is denied.
   */
  void assertAccess(Type operationType, UserGroupInformation ugi,
      KMSOp operation, String key) throws AccessControlException {
    if (!hasAccess(operationType, ugi)) {
      KMSWebApp.getUnauthorizedCallsMeter().mark();
      KMSWebApp.getKMSAudit().unauthorized(ugi, operation, key);
      throw new AuthorizationException(String.format(
          (key != null) ? UNAUTHORIZED_MSG_WITH_KEY
                        : UNAUTHORIZED_MSG_WITHOUT_KEY,
          ugi.getShortUserName(), operation, key));
    }
  }

}
