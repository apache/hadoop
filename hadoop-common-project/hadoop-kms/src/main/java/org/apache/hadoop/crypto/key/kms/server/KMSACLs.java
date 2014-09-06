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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Provides access to the <code>AccessControlList</code>s used by KMS,
 * hot-reloading them if the <code>kms-acls.xml</code> file where the ACLs
 * are defined has been updated.
 */
@InterfaceAudience.Private
public class KMSACLs implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(KMSACLs.class);

  private static final String UNAUTHORIZED_MSG_WITH_KEY =
      "User:%s not allowed to do '%s' on '%s'";

  private static final String UNAUTHORIZED_MSG_WITHOUT_KEY =
      "User:%s not allowed to do '%s'";

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

  public static final String ACL_DEFAULT = AccessControlList.WILDCARD_ACL_VALUE;

  public static final int RELOADER_SLEEP_MILLIS = 1000;

  private volatile Map<Type, AccessControlList> acls;
  private volatile Map<Type, AccessControlList> blacklistedAcls;
  private ScheduledExecutorService executorService;
  private long lastReload;

  KMSACLs(Configuration conf) {
    if (conf == null) {
      conf = loadACLs();
    }
    setACLs(conf);
  }

  public KMSACLs() {
    this(null);
  }

  private void setACLs(Configuration conf) {
    Map<Type, AccessControlList> tempAcls = new HashMap<Type, AccessControlList>();
    Map<Type, AccessControlList> tempBlacklist = new HashMap<Type, AccessControlList>();
    for (Type aclType : Type.values()) {
      String aclStr = conf.get(aclType.getAclConfigKey(), ACL_DEFAULT);
      tempAcls.put(aclType, new AccessControlList(aclStr));
      String blacklistStr = conf.get(aclType.getBlacklistConfigKey());
      if (blacklistStr != null) {
        // Only add if blacklist is present
        tempBlacklist.put(aclType, new AccessControlList(blacklistStr));
        LOG.info("'{}' Blacklist '{}'", aclType, blacklistStr);
      }
      LOG.info("'{}' ACL '{}'", aclType, aclStr);
    }
    acls = tempAcls;
    blacklistedAcls = tempBlacklist;
  }

  @Override
  public void run() {
    try {
      if (KMSConfiguration.isACLsFileNewer(lastReload)) {
        setACLs(loadACLs());
      }
    } catch (Exception ex) {
      LOG.warn("Could not reload ACLs file: " + ex.toString(), ex);
    }
  }

  public synchronized void startReloader() {
    if (executorService == null) {
      executorService = Executors.newScheduledThreadPool(1);
      executorService.scheduleAtFixedRate(this, RELOADER_SLEEP_MILLIS,
          RELOADER_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  public synchronized void stopReloader() {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  private Configuration loadACLs() {
    LOG.debug("Loading ACLs file");
    lastReload = System.currentTimeMillis();
    Configuration conf = KMSConfiguration.getACLsConf();
    // triggering the resource loading.
    conf.get(Type.CREATE.getAclConfigKey());
    return conf;
  }

  /**
   * First Check if user is in ACL for the KMS operation, if yes, then
   * return true if user is not present in any configured blacklist for
   * the operation
   * @param type KMS Operation
   * @param ugi UserGroupInformation of user
   * @return true is user has access
   */
  public boolean hasAccess(Type type, UserGroupInformation ugi) {
    boolean access = acls.get(type).isUserAllowed(ugi);
    if (access) {
      AccessControlList blacklist = blacklistedAcls.get(type);
      access = (blacklist == null) || !blacklist.isUserInList(ugi);
    }
    return access;
  }

  public void assertAccess(KMSACLs.Type aclType,
      UserGroupInformation ugi, KMSOp operation, String key)
      throws AccessControlException {
    if (!KMSWebApp.getACLs().hasAccess(aclType, ugi)) {
      KMSWebApp.getUnauthorizedCallsMeter().mark();
      KMSWebApp.getKMSAudit().unauthorized(ugi, operation, key);
      throw new AuthorizationException(String.format(
          (key != null) ? UNAUTHORIZED_MSG_WITH_KEY
                        : UNAUTHORIZED_MSG_WITHOUT_KEY,
          ugi.getShortUserName(), operation, key));
    }
  }

}
