/*
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

package org.apache.hadoop.hbase.security.access;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Performs authorization checks for a given user's assigned permissions
 */
public class TableAuthManager {
  /** Key for the user and group cache maps for globally assigned permissions */
  private static final String GLOBAL_CACHE_KEY = ".access.";
  private static Log LOG = LogFactory.getLog(TableAuthManager.class);

  private static TableAuthManager instance;

  /** Cache of global user permissions */
  private ListMultimap<String,Permission> USER_CACHE = ArrayListMultimap.create();
  /** Cache of global group permissions */
  private ListMultimap<String,Permission> GROUP_CACHE = ArrayListMultimap.create();

  private ConcurrentSkipListMap<byte[], ListMultimap<String,TablePermission>> TABLE_USER_CACHE =
      new ConcurrentSkipListMap<byte[], ListMultimap<String,TablePermission>>(Bytes.BYTES_COMPARATOR);

  private ConcurrentSkipListMap<byte[], ListMultimap<String,TablePermission>> TABLE_GROUP_CACHE =
      new ConcurrentSkipListMap<byte[], ListMultimap<String,TablePermission>>(Bytes.BYTES_COMPARATOR);

  private Configuration conf;
  private ZKPermissionWatcher zkperms;

  private TableAuthManager(ZooKeeperWatcher watcher, Configuration conf)
      throws IOException {
    this.conf = conf;
    this.zkperms = new ZKPermissionWatcher(watcher, this, conf);
    try {
      this.zkperms.start();
    } catch (KeeperException ke) {
      LOG.error("ZooKeeper initialization failed", ke);
    }

    // initialize global permissions based on configuration
    initGlobal(conf);
  }

  private void initGlobal(Configuration conf) throws IOException {
    User user = User.getCurrent();
    if (user == null) {
      throw new IOException("Unable to obtain the current user, " +
          "authorization checks for internal operations will not work correctly!");
    }
    String currentUser = user.getShortName();

    // the system user is always included
    List<String> superusers = Lists.asList(currentUser, conf.getStrings(
        AccessControlLists.SUPERUSER_CONF_KEY, new String[0]));
    if (superusers != null) {
      for (String name : superusers) {
        if (AccessControlLists.isGroupPrincipal(name)) {
          GROUP_CACHE.put(AccessControlLists.getGroupName(name),
              new Permission(Permission.Action.values()));
        } else {
          USER_CACHE.put(name, new Permission(Permission.Action.values()));
        }
      }
    }
  }

  public ZKPermissionWatcher getZKPermissionWatcher() {
    return this.zkperms;
  }

  public void refreshCacheFromWritable(byte[] table, byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      DataInput in = new DataInputStream( new ByteArrayInputStream(data) );
      ListMultimap<String,TablePermission> perms = AccessControlLists.readPermissions(in, conf);
      cache(table, perms);
    } else {
      LOG.debug("Skipping permission cache refresh because writable data is empty");
    }
  }

  /**
   * Updates the internal permissions cache for a single table, splitting
   * the permissions listed into separate caches for users and groups to optimize
   * group lookups.
   * 
   * @param table
   * @param tablePerms
   */
  private void cache(byte[] table,
      ListMultimap<String,TablePermission> tablePerms) {
    // split user from group assignments so we don't have to prepend the group
    // prefix every time we query for groups
    ListMultimap<String,TablePermission> userPerms = ArrayListMultimap.create();
    ListMultimap<String,TablePermission> groupPerms = ArrayListMultimap.create();

    if (tablePerms != null) {
      for (Map.Entry<String,TablePermission> entry : tablePerms.entries()) {
        if (AccessControlLists.isGroupPrincipal(entry.getKey())) {
          groupPerms.put(
              entry.getKey().substring(AccessControlLists.GROUP_PREFIX.length()),
              entry.getValue());
        } else {
          userPerms.put(entry.getKey(), entry.getValue());
        }
      }
      TABLE_GROUP_CACHE.put(table, groupPerms);
      TABLE_USER_CACHE.put(table, userPerms);
    }
  }

  private List<TablePermission> getUserPermissions(String username, byte[] table) {
    ListMultimap<String, TablePermission> tablePerms = TABLE_USER_CACHE.get(table);
    if (tablePerms != null) {
      return tablePerms.get(username);
    }

    return null;
  }

  private List<TablePermission> getGroupPermissions(String groupName, byte[] table) {
    ListMultimap<String, TablePermission> tablePerms = TABLE_GROUP_CACHE.get(table);
    if (tablePerms != null) {
      return tablePerms.get(groupName);
    }

    return null;
  }

  /**
   * Authorizes a global permission
   * @param perms
   * @param action
   * @return
   */
  private boolean authorize(List<Permission> perms, Permission.Action action) {
    if (perms != null) {
      for (Permission p : perms) {
        if (p.implies(action)) {
          return true;
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No permissions found");
    }

    return false;
  }

  /**
   * Authorize a global permission based on ACLs for the given user and the
   * user's groups.
   * @param user
   * @param action
   * @return
   */
  public boolean authorize(User user, Permission.Action action) {
    if (user == null) {
      return false;
    }

    if (authorize(USER_CACHE.get(user.getShortName()), action)) {
      return true;
    }

    String[] groups = user.getGroupNames();
    if (groups != null) {
      for (String group : groups) {
        if (authorize(GROUP_CACHE.get(group), action)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean authorize(List<TablePermission> perms, byte[] table, byte[] family,
      Permission.Action action) {
    return authorize(perms, table, family, null, action);
  }

  private boolean authorize(List<TablePermission> perms, byte[] table, byte[] family,
      byte[] qualifier, Permission.Action action) {
    if (perms != null) {
      for (TablePermission p : perms) {
        if (p.implies(table, family, qualifier, action)) {
          return true;
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No permissions found for table="+Bytes.toStringBinary(table));
    }
    return false;
  }

  public boolean authorize(User user, byte[] table, KeyValue kv,
      TablePermission.Action action) {
    List<TablePermission> userPerms = getUserPermissions(
        user.getShortName(), table);
    if (authorize(userPerms, table, kv, action)) {
      return true;
    }

    String[] groupNames = user.getGroupNames();
    if (groupNames != null) {
      for (String group : groupNames) {
        List<TablePermission> groupPerms = getGroupPermissions(group, table);
        if (authorize(groupPerms, table, kv, action)) {
          return true;
        }
      }
    }

    return false;
  }

  private boolean authorize(List<TablePermission> perms, byte[] table, KeyValue kv,
      TablePermission.Action action) {
    if (perms != null) {
      for (TablePermission p : perms) {
        if (p.implies(table, kv, action)) {
          return true;
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No permissions for authorize() check, table=" +
          Bytes.toStringBinary(table));
    }

    return false;
  }

  /**
   * Checks global authorization for a specific action for a user, based on the
   * stored user permissions.
   */
  public boolean authorizeUser(String username, Permission.Action action) {
    return authorize(USER_CACHE.get(username), action);
  }

  /**
   * Checks authorization to a given table and column family for a user, based on the
   * stored user permissions.
   *
   * @param username
   * @param table
   * @param family
   * @param action
   * @return
   */
  public boolean authorizeUser(String username, byte[] table, byte[] family,
      Permission.Action action) {
    return authorizeUser(username, table, family, null, action);
  }

  public boolean authorizeUser(String username, byte[] table, byte[] family,
      byte[] qualifier, Permission.Action action) {
    // global authorization supercedes table level
    if (authorizeUser(username, action)) {
      return true;
    }
    return authorize(getUserPermissions(username, table), table, family,
        qualifier, action);
  }


  /**
   * Checks authorization for a given action for a group, based on the stored
   * permissions.
   */
  public boolean authorizeGroup(String groupName, Permission.Action action) {
    return authorize(GROUP_CACHE.get(groupName), action);
  }

  /**
   * Checks authorization to a given table and column family for a group, based
   * on the stored permissions. 
   * @param groupName
   * @param table
   * @param family
   * @param action
   * @return
   */
  public boolean authorizeGroup(String groupName, byte[] table, byte[] family,
      Permission.Action action) {
    // global authorization supercedes table level
    if (authorizeGroup(groupName, action)) {
      return true;
    }
    return authorize(getGroupPermissions(groupName, table), table, family, action);
  }

  public boolean authorize(User user, byte[] table, byte[] family,
      byte[] qualifier, Permission.Action action) {
    if (authorizeUser(user.getShortName(), table, family, qualifier, action)) {
      return true;
    }

    String[] groups = user.getGroupNames();
    if (groups != null) {
      for (String group : groups) {
        if (authorizeGroup(group, table, family, action)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean authorize(User user, byte[] table, byte[] family,
      Permission.Action action) {
    return authorize(user, table, family, null, action);
  }

  /**
   * Returns true if the given user has a {@link TablePermission} matching up
   * to the column family portion of a permission.  Note that this permission
   * may be scoped to a given column qualifier and does not guarantee that
   * authorize() on the same column family would return true.
   */
  public boolean matchPermission(User user,
      byte[] table, byte[] family, TablePermission.Action action) {
    List<TablePermission> userPerms = getUserPermissions(
        user.getShortName(), table);
    if (userPerms != null) {
      for (TablePermission p : userPerms) {
        if (p.matchesFamily(table, family, action)) {
          return true;
        }
      }
    }

    String[] groups = user.getGroupNames();
    if (groups != null) {
      for (String group : groups) {
        List<TablePermission> groupPerms = getGroupPermissions(group, table);
        if (groupPerms != null) {
          for (TablePermission p : groupPerms) {
            if (p.matchesFamily(table, family, action)) {
              return true;
            }
          }
        }
      }
    }

    return false;
  }

  public boolean matchPermission(User user,
      byte[] table, byte[] family, byte[] qualifier,
      TablePermission.Action action) {
    List<TablePermission> userPerms = getUserPermissions(
        user.getShortName(), table);
    if (userPerms != null) {
      for (TablePermission p : userPerms) {
        if (p.matchesFamilyQualifier(table, family, qualifier, action)) {
          return true;
        }
      }
    }

    String[] groups = user.getGroupNames();
    if (groups != null) {
      for (String group : groups) {
        List<TablePermission> groupPerms = getGroupPermissions(group, table);
        if (groupPerms != null) {
          for (TablePermission p : groupPerms) {
            if (p.matchesFamilyQualifier(table, family, qualifier, action)) {
              return true;
            }
          }
        }
      }
    }

    return false;
  }

  public void remove(byte[] table) {
    TABLE_USER_CACHE.remove(table);
    TABLE_GROUP_CACHE.remove(table);
  }

  /**
   * Overwrites the existing permission set for a given user for a table, and
   * triggers an update for zookeeper synchronization.
   * @param username
   * @param table
   * @param perms
   */
  public void setUserPermissions(String username, byte[] table,
      List<TablePermission> perms) {
    ListMultimap<String,TablePermission> tablePerms = TABLE_USER_CACHE.get(table);
    if (tablePerms == null) {
      tablePerms = ArrayListMultimap.create();
      TABLE_USER_CACHE.put(table, tablePerms);
    }
    tablePerms.replaceValues(username, perms);
    writeToZooKeeper(table, tablePerms, TABLE_GROUP_CACHE.get(table));
  }

  /**
   * Overwrites the existing permission set for a group and triggers an update
   * for zookeeper synchronization.
   * @param group
   * @param table
   * @param perms
   */
  public void setGroupPermissions(String group, byte[] table,
      List<TablePermission> perms) {
    ListMultimap<String,TablePermission> tablePerms = TABLE_GROUP_CACHE.get(table);
    if (tablePerms == null) {
      tablePerms = ArrayListMultimap.create();
      TABLE_GROUP_CACHE.put(table, tablePerms);
    }
    tablePerms.replaceValues(group, perms);
    writeToZooKeeper(table, TABLE_USER_CACHE.get(table), tablePerms);
  }

  public void writeToZooKeeper(byte[] table,
      ListMultimap<String,TablePermission> userPerms,
      ListMultimap<String,TablePermission> groupPerms) {
    ListMultimap<String,TablePermission> tmp = ArrayListMultimap.create();
    if (userPerms != null) {
      tmp.putAll(userPerms);
    }
    if (groupPerms != null) {
      for (String group : groupPerms.keySet()) {
        tmp.putAll(AccessControlLists.GROUP_PREFIX + group,
            groupPerms.get(group));
      }
    }
    byte[] serialized = AccessControlLists.writePermissionsAsBytes(tmp, conf);
    zkperms.writeToZookeeper(Bytes.toString(table), serialized);
  }

  static Map<ZooKeeperWatcher,TableAuthManager> managerMap =
    new HashMap<ZooKeeperWatcher,TableAuthManager>();

  public synchronized static TableAuthManager get(
      ZooKeeperWatcher watcher, Configuration conf) throws IOException {
    instance = managerMap.get(watcher);
    if (instance == null) {
      instance = new TableAuthManager(watcher, conf);
      managerMap.put(watcher, instance);
    }
    return instance;
  }
}
