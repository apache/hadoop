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
package org.apache.hadoop.nfs.nfs3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * Map id to user name or group name. It does update every 15 minutes. Only a
 * single instance of this class is expected to be on the server.
 */
public class IdUserGroup {
  static final Log LOG = LogFactory.getLog(IdUserGroup.class);
  private final static String OS = System.getProperty("os.name");

  /** Shell commands to get users and groups */
  static final String LINUX_GET_ALL_USERS_CMD = "getent passwd | cut -d: -f1,3";
  static final String LINUX_GET_ALL_GROUPS_CMD = "getent group | cut -d: -f1,3";
  static final String MAC_GET_ALL_USERS_CMD = "dscl . -list /Users UniqueID";
  static final String MAC_GET_ALL_GROUPS_CMD = "dscl . -list /Groups PrimaryGroupID";

  // Do update every 15 minutes by default
  final static long TIMEOUT_DEFAULT = 15 * 60 * 1000; // ms
  final static long TIMEOUT_MIN = 1 * 60 * 1000; // ms
  final private long timeout;
  final static String NFS_USERUPDATE_MILLY = "hadoop.nfs.userupdate.milly";
  
  // Maps for id to name map. Guarded by this object monitor lock
  private BiMap<Integer, String> uidNameMap = HashBiMap.create();
  private BiMap<Integer, String> gidNameMap = HashBiMap.create();

  private long lastUpdateTime = 0; // Last time maps were updated
  
  public IdUserGroup() throws IOException {
    timeout = TIMEOUT_DEFAULT;
    updateMaps();
  }
  
  public IdUserGroup(Configuration conf) throws IOException {
    long updateTime = conf.getLong(NFS_USERUPDATE_MILLY, TIMEOUT_DEFAULT);
    // Minimal interval is 1 minute
    if (updateTime < TIMEOUT_MIN) {
      LOG.info("User configured user account update time is less"
          + " than 1 minute. Use 1 minute instead.");
      timeout = TIMEOUT_MIN;
    } else {
      timeout = updateTime;
    }
    updateMaps();
  }

  @VisibleForTesting
  public long getTimeout() {
    return timeout;
  }
  
  synchronized private boolean isExpired() {
    return lastUpdateTime - System.currentTimeMillis() > timeout;
  }

  // If can't update the maps, will keep using the old ones
  private void checkAndUpdateMaps() {
    if (isExpired()) {
      LOG.info("Update cache now");
      try {
        updateMaps();
      } catch (IOException e) {
        LOG.error("Can't update the maps. Will use the old ones,"
            + " which can potentially cause problem.", e);
      }
    }
  }

  private static final String DUPLICATE_NAME_ID_DEBUG_INFO =
      "NFS gateway could have problem starting with duplicate name or id on the host system.\n"
      + "This is because HDFS (non-kerberos cluster) uses name as the only way to identify a user or group.\n"
      + "The host system with duplicated user/group name or id might work fine most of the time by itself.\n"
      + "However when NFS gateway talks to HDFS, HDFS accepts only user and group name.\n"
      + "Therefore, same name means the same user or same group. To find the duplicated names/ids, one can do:\n"
      + "<getent passwd | cut -d: -f1,3> and <getent group | cut -d: -f1,3> on Linux systms,\n"
      + "<dscl . -list /Users UniqueID> and <dscl . -list /Groups PrimaryGroupID> on MacOS.";
  
  private static void reportDuplicateEntry(final String header,
      final Integer key, final String value,
      final Integer ekey, final String evalue) {    
      LOG.warn("\n" + header + String.format(
          "new entry (%d, %s), existing entry: (%d, %s).\n%s\n%s",
          key, value, ekey, evalue,
          "The new entry is to be ignored for the following reason.",
          DUPLICATE_NAME_ID_DEBUG_INFO));
  }
      
  /**
   * Get the whole list of users and groups and save them in the maps.
   * @throws IOException 
   */
  @VisibleForTesting
  public static void updateMapInternal(BiMap<Integer, String> map, String mapName,
      String command, String regex) throws IOException  {
    BufferedReader br = null;
    try {
      Process process = Runtime.getRuntime().exec(
          new String[] { "bash", "-c", command });
      br = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] nameId = line.split(regex);
        if ((nameId == null) || (nameId.length != 2)) {
          throw new IOException("Can't parse " + mapName + " list entry:" + line);
        }
        LOG.debug("add to " + mapName + "map:" + nameId[0] + " id:" + nameId[1]);
        // HDFS can't differentiate duplicate names with simple authentication
        final Integer key = Integer.valueOf(nameId[1]);
        final String value = nameId[0];        
        if (map.containsKey(key)) {
          final String prevValue = map.get(key);
          if (value.equals(prevValue)) {
            // silently ignore equivalent entries
            continue;
          }
          reportDuplicateEntry(
              "Got multiple names associated with the same id: ",
              key, value, key, prevValue);           
          continue;
        }
        if (map.containsValue(value)) {
          final Integer prevKey = map.inverse().get(value);
          reportDuplicateEntry(
              "Got multiple ids associated with the same name: ",
              key, value, prevKey, value);
          continue;
        }
        map.put(key, value);
      }
      LOG.info("Updated " + mapName + " map size:" + map.size());
      
    } catch (IOException e) {
      LOG.error("Can't update " + mapName + " map");
      throw e;
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e1) {
          LOG.error("Can't close BufferedReader of command result", e1);
        }
      }
    }
  }

  synchronized public void updateMaps() throws IOException {
    BiMap<Integer, String> uMap = HashBiMap.create();
    BiMap<Integer, String> gMap = HashBiMap.create();

    if (!OS.startsWith("Linux") && !OS.startsWith("Mac")) {
      LOG.error("Platform is not supported:" + OS
          + ". Can't update user map and group map and"
          + " 'nobody' will be used for any user and group.");
      return;
    }

    if (OS.startsWith("Linux")) {
      updateMapInternal(uMap, "user", LINUX_GET_ALL_USERS_CMD, ":");
      updateMapInternal(gMap, "group", LINUX_GET_ALL_GROUPS_CMD, ":");
    } else {
      // Mac
      updateMapInternal(uMap, "user", MAC_GET_ALL_USERS_CMD, "\\s+");
      updateMapInternal(gMap, "group", MAC_GET_ALL_GROUPS_CMD, "\\s+");
    }

    uidNameMap = uMap;
    gidNameMap = gMap;
    lastUpdateTime = System.currentTimeMillis();
  }

  synchronized public int getUid(String user) throws IOException {
    checkAndUpdateMaps();

    Integer id = uidNameMap.inverse().get(user);
    if (id == null) {
      throw new IOException("User just deleted?:" + user);
    }
    return id.intValue();
  }

  synchronized public int getGid(String group) throws IOException {
    checkAndUpdateMaps();

    Integer id = gidNameMap.inverse().get(group);
    if (id == null) {
      throw new IOException("No such group:" + group);

    }
    return id.intValue();
  }

  synchronized public String getUserName(int uid, String unknown) {
    checkAndUpdateMaps();
    String uname = uidNameMap.get(uid);
    if (uname == null) {
      LOG.warn("Can't find user name for uid " + uid
          + ". Use default user name " + unknown);
      uname = unknown;
    }
    return uname;
  }

  synchronized public String getGroupName(int gid, String unknown) {
    checkAndUpdateMaps();
    String gname = gidNameMap.get(gid);
    if (gname == null) {
      LOG.warn("Can't find group name for gid " + gid
          + ". Use default group name " + unknown);
      gname = unknown;
    }
    return gname;
  }

  // When can't map user, return user name's string hashcode
  public int getUidAllowingUnknown(String user) {
    checkAndUpdateMaps();
    int uid;
    try {
      uid = getUid(user);
    } catch (IOException e) {
      uid = user.hashCode();
      LOG.info("Can't map user " + user + ". Use its string hashcode:" + uid, e);
    }
    return uid;
  }

  // When can't map group, return group name's string hashcode
  public int getGidAllowingUnknown(String group) {
    checkAndUpdateMaps();
    int gid;
    try {
      gid = getGid(group);
    } catch (IOException e) {
      gid = group.hashCode();
      LOG.info("Can't map group " + group + ". Use its string hashcode:" + gid, e);
    }
    return gid;
  }
}
