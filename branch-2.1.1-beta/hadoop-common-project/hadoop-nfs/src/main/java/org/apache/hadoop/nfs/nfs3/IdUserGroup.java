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

  // Maps for id to name map. Guarded by this object monitor lock */
  private BiMap<Integer, String> uidNameMap = HashBiMap.create();
  private BiMap<Integer, String> gidNameMap = HashBiMap.create();

  private long lastUpdateTime = 0; // Last time maps were updated

  public IdUserGroup() {
    updateMaps();
  }

  private boolean isExpired() {
    return lastUpdateTime - System.currentTimeMillis() > TIMEOUT;
  }

  private void checkAndUpdateMaps() {
    if (isExpired()) {
      LOG.info("Update cache now");
      updateMaps();
    }
  }

  /**
   * Get the whole list of users and groups and save them in the maps.
   */
  private void updateMapInternal(BiMap<Integer, String> map, String name,
      String command, String regex) throws IOException {
    BufferedReader br = null;
    try {
      Process process = Runtime.getRuntime().exec(
          new String[] { "bash", "-c", command });
      br = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] nameId = line.split(regex);
        if ((nameId == null) || (nameId.length != 2)) {
          throw new IOException("Can't parse " + name + " list entry:" + line);
        }
        LOG.debug("add " + name + ":" + nameId[0] + " id:" + nameId[1]);
        map.put(Integer.valueOf(nameId[1]), nameId[0]);
      }
      LOG.info("Updated " + name + " map size:" + map.size());
      
    } catch (IOException e) {
      LOG.error("Can't update map " + name);
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

  synchronized public void updateMaps() {
    BiMap<Integer, String> uMap = HashBiMap.create();
    BiMap<Integer, String> gMap = HashBiMap.create();

    try {
      if (OS.startsWith("Linux")) {
        updateMapInternal(uMap, "user", LINUX_GET_ALL_USERS_CMD, ":");
        updateMapInternal(gMap, "group", LINUX_GET_ALL_GROUPS_CMD, ":");
      } else if (OS.startsWith("Mac")) {
        updateMapInternal(uMap, "user", MAC_GET_ALL_USERS_CMD, "\\s+");
        updateMapInternal(gMap, "group", MAC_GET_ALL_GROUPS_CMD, "\\s+");
      } else {
        throw new IOException("Platform is not supported:" + OS);
      }
    } catch (IOException e) {
      LOG.error("Can't update maps:" + e);
      return;
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
