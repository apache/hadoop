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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides access to the <code>AccessControlList</code>s used by KMS,
 * hot-reloading them if the <code>kms-acls.xml</code> file where the ACLs
 * are defined has been updated.
 */
public class KMSACLs implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(KMSACLs.class);


  public enum Type {
    CREATE, DELETE, ROLLOVER, GET, GET_KEYS, GET_METADATA, SET_KEY_MATERIAL;

    public String getConfigKey() {
      return KMSConfiguration.CONFIG_PREFIX + "acl." + this.toString();
    }
  }

  public static final String ACL_DEFAULT = AccessControlList.WILDCARD_ACL_VALUE;

  public static final int RELOADER_SLEEP_MILLIS = 1000;

  Map<Type, AccessControlList> acls;
  private ReadWriteLock lock;
  private ScheduledExecutorService executorService;
  private long lastReload;

  KMSACLs(Configuration conf) {
    lock = new ReentrantReadWriteLock();
    if (conf == null) {
      conf = loadACLs();
    }
    setACLs(conf);
  }

  public KMSACLs() {
    this(null);
  }

  private void setACLs(Configuration conf) {
    lock.writeLock().lock();
    try {
      acls = new HashMap<Type, AccessControlList>();
      for (Type aclType : Type.values()) {
        String aclStr = conf.get(aclType.getConfigKey(), ACL_DEFAULT);
        acls.put(aclType, new AccessControlList(aclStr));
        LOG.info("'{}' ACL '{}'", aclType, aclStr);
      }
    } finally {
      lock.writeLock().unlock();
    }
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
    conf.get(Type.CREATE.getConfigKey());
    return conf;
  }

  public boolean hasAccess(Type type, String user) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    AccessControlList acl = null;
    lock.readLock().lock();
    try {
      acl = acls.get(type);
    } finally {
      lock.readLock().unlock();
    }
    return acl.isUserAllowed(ugi);
  }

}
