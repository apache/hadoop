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

  // Do update every 15 minutes
  final static long TIMEOUT = 15 * 60 * 1000; // ms

  // Maps for id to name map. Guarded by this object monitor lock
  private BiMap<Integer, String> uidNameMap = HashBiMap.create();
  private BiMap<Integer, String> gidNameMap = HashBiMap.create();

  private long lastUpdateTime = 0; // Last time maps were updated

  static public class DuplicateNameOrIdException extends IOException {
    private static final long serialVersionUID = 1L;

    public DuplicateNameOrIdException(String msg) {
      super(msg);
    }
  }
  
  public IdUserGroup() throws IOException {
    updateMaps();
  }

  private boolean isExpired() {
    return lastUpdateTime - System.currentTimeMillis() > TIMEOUT;
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

  private static final String DUPLICATE_NAME_ID_DEBUG_INFO = "NFS gateway can't start with duplicate name or id on the host system.\n"
      + "This is because HDFS (non-kerberos cluster) uses name as the only way to identify a user or group.\n"
      + "The host system with duplicated user/group name or id might work fine most of the time by itself.\n"
      + "However when NFS gateway talks to HDFS, HDFS accepts only user and group name.\n"
      + "Therefore, same name means the same user or same group. To find the duplicated names/ids, one can do:\n"
      + "<getent passwd | cut -d: -f1,3> and <getent group | cut -d: -f1,3> on Linux systms,\n"
      + "<dscl . -list /Users UniqueID> and <dscl . -list /Groups PrimaryGroupID> on MacOS.";
  
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
        Integer key = Integer.valueOf(nameId[1]);
        String value = nameId[0];
        if (map.containsKey(key)) {
          LOG.error(String.format(
              "Got duplicate id:(%d, %s), existing entry: (%d, %s).\n%s", key,
              value, key, map.get(key), DUPLICATE_NAME_ID_DEBUG_INFO));
          throw new DuplicateNameOrIdException("Got duplicate id.");
        }
        if (map.containsValue(nameId[0])) {
          LOG.error(String.format(
              "Got duplicate name:(%d, %s), existing entry: (%d, %s) \n%s",
              key, value, map.inverse().get(value), value,
              DUPLICATE_NAME_ID_DEBUG_INFO));
          throw new DuplicateNameOrIdException("Got duplicate name");
        }
        map.put(Integer.valueOf(nameId[1]), nameId[0]);
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
          LOG.error("Can't close BufferedReader of command result");
          e1.printStackTrace();
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
      LOG.info("Can't map user " + user + ". Use its string hashcode:" + uid);
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
      LOG.debug("Can't map group " + group + ". Use its string hashcode:" + gid);
    }
    return gid;
  }
}
