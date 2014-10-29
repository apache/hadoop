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
package org.apache.hadoop.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * A simple shell-based implementation of {@link IdMappingServiceProvider} 
 * Map id to user name or group name. It does update every 15 minutes. Only a
 * single instance of this class is expected to be on the server.
 */
public class ShellBasedIdMapping implements IdMappingServiceProvider {

  private static final Log LOG =
      LogFactory.getLog(ShellBasedIdMapping.class);

  private final static String OS = System.getProperty("os.name");

  /** Shell commands to get users and groups */
  static final String GET_ALL_USERS_CMD = "getent passwd | cut -d: -f1,3";
  static final String GET_ALL_GROUPS_CMD = "getent group | cut -d: -f1,3";
  static final String MAC_GET_ALL_USERS_CMD = "dscl . -list /Users UniqueID";
  static final String MAC_GET_ALL_GROUPS_CMD = "dscl . -list /Groups PrimaryGroupID";

  private final File staticMappingFile;

  // Used for parsing the static mapping file.
  private static final Pattern EMPTY_LINE = Pattern.compile("^\\s*$");
  private static final Pattern COMMENT_LINE = Pattern.compile("^\\s*#.*$");
  private static final Pattern MAPPING_LINE =
      Pattern.compile("^(uid|gid)\\s+(\\d+)\\s+(\\d+)\\s*(#.*)?$");

  final private long timeout;
  
  // Maps for id to name map. Guarded by this object monitor lock
  private BiMap<Integer, String> uidNameMap = HashBiMap.create();
  private BiMap<Integer, String> gidNameMap = HashBiMap.create();

  private long lastUpdateTime = 0; // Last time maps were updated
  
  public ShellBasedIdMapping(Configuration conf,
      final String defaultStaticIdMappingFile) throws IOException {
    long updateTime = conf.getLong(
        IdMappingConstant.USERGROUPID_UPDATE_MILLIS_KEY,
        IdMappingConstant.USERGROUPID_UPDATE_MILLIS_DEFAULT);
    // Minimal interval is 1 minute
    if (updateTime < IdMappingConstant.USERGROUPID_UPDATE_MILLIS_MIN) {
      LOG.info("User configured user account update time is less"
          + " than 1 minute. Use 1 minute instead.");
      timeout = IdMappingConstant.USERGROUPID_UPDATE_MILLIS_MIN;
    } else {
      timeout = updateTime;
    }
    
    String staticFilePath = conf.get(IdMappingConstant.STATIC_ID_MAPPING_FILE_KEY,
        defaultStaticIdMappingFile);
    staticMappingFile = new File(staticFilePath);
    
    updateMaps();
  }

  public ShellBasedIdMapping(Configuration conf) throws IOException {
    this(conf, IdMappingConstant.STATIC_ID_MAPPING_FILE_DEFAULT);
  }

  @VisibleForTesting
  public long getTimeout() {
    return timeout;
  }
  
  synchronized private boolean isExpired() {
    return Time.monotonicNow() - lastUpdateTime > timeout;
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
      + "<getent passwd | cut -d: -f1,3> and <getent group | cut -d: -f1,3> on Linux systems,\n"
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
   * uid and gid are defined as uint32 in linux. Some systems create
   * (intended or unintended) <nfsnobody, 4294967294> kind of <name,Id>
   * mapping, where 4294967294 is 2**32-2 as unsigned int32. As an example,
   *   https://bugzilla.redhat.com/show_bug.cgi?id=511876.
   * Because user or group id are treated as Integer (signed integer or int32)
   * here, the number 4294967294 is out of range. The solution is to convert
   * uint32 to int32, so to map the out-of-range ID to the negative side of
   * Integer, e.g. 4294967294 maps to -2 and 4294967295 maps to -1.
   */
  private static Integer parseId(final String idStr) {
    Long longVal = Long.parseLong(idStr);
    int intVal = longVal.intValue();
    return Integer.valueOf(intVal);
  }
  
  /**
   * Get the whole list of users and groups and save them in the maps.
   * @throws IOException 
   */
  @VisibleForTesting
  public static void updateMapInternal(BiMap<Integer, String> map, String mapName,
      String command, String regex, Map<Integer, Integer> staticMapping)
      throws IOException  {
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
        final Integer key = staticMapping.get(parseId(nameId[1]));
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
      LOG.info("Updated " + mapName + " map size: " + map.size());
      
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
    
    StaticMapping staticMapping = new StaticMapping(
        new HashMap<Integer, Integer>(), new HashMap<Integer, Integer>());
    if (staticMappingFile.exists()) {
      LOG.info("Using '" + staticMappingFile + "' for static UID/GID mapping...");
      staticMapping = parseStaticMap(staticMappingFile);
    } else {
      LOG.info("Not doing static UID/GID mapping because '" + staticMappingFile
          + "' does not exist.");
    }

    if (OS.startsWith("Mac")) {
      updateMapInternal(uMap, "user", MAC_GET_ALL_USERS_CMD, "\\s+",
          staticMapping.uidMapping);
      updateMapInternal(gMap, "group", MAC_GET_ALL_GROUPS_CMD, "\\s+",
          staticMapping.gidMapping);
    } else {
      updateMapInternal(uMap, "user", GET_ALL_USERS_CMD, ":",
          staticMapping.uidMapping);
      updateMapInternal(gMap, "group", GET_ALL_GROUPS_CMD, ":",
          staticMapping.gidMapping);
    }

    uidNameMap = uMap;
    gidNameMap = gMap;
    lastUpdateTime = Time.monotonicNow();
  }
  
  @SuppressWarnings("serial")
  static final class PassThroughMap<K> extends HashMap<K, K> {
    
    public PassThroughMap() {
      this(new HashMap<K, K>());
    }
    
    public PassThroughMap(Map<K, K> mapping) {
      super();
      for (Map.Entry<K, K> entry : mapping.entrySet()) {
        super.put(entry.getKey(), entry.getValue());
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public K get(Object key) {
      if (super.containsKey(key)) {
        return super.get(key);
      } else {
        return (K) key;
      }
    }
  }
  
  @VisibleForTesting
  static final class StaticMapping {
    final Map<Integer, Integer> uidMapping;
    final Map<Integer, Integer> gidMapping;
    
    public StaticMapping(Map<Integer, Integer> uidMapping,
        Map<Integer, Integer> gidMapping) {
      this.uidMapping = new PassThroughMap<Integer>(uidMapping);
      this.gidMapping = new PassThroughMap<Integer>(gidMapping);
    }
  }
  
  static StaticMapping parseStaticMap(File staticMapFile)
      throws IOException {
    
    Map<Integer, Integer> uidMapping = new HashMap<Integer, Integer>();
    Map<Integer, Integer> gidMapping = new HashMap<Integer, Integer>();
    
    BufferedReader in = new BufferedReader(new InputStreamReader(
        new FileInputStream(staticMapFile)));
    
    try {
      String line = null;
      while ((line = in.readLine()) != null) {
        // Skip entirely empty and comment lines.
        if (EMPTY_LINE.matcher(line).matches() ||
            COMMENT_LINE.matcher(line).matches()) {
          continue;
        }
        
        Matcher lineMatcher = MAPPING_LINE.matcher(line);
        if (!lineMatcher.matches()) {
          LOG.warn("Could not parse line '" + line + "'. Lines should be of " +
              "the form '[uid|gid] [remote id] [local id]'. Blank lines and " +
              "everything following a '#' on a line will be ignored.");
          continue;
        }
        
        // We know the line is fine to parse without error checking like this
        // since it matched the regex above.
        String firstComponent = lineMatcher.group(1);
        int remoteId = Integer.parseInt(lineMatcher.group(2));
        int localId = Integer.parseInt(lineMatcher.group(3));
        if (firstComponent.equals("uid")) {
          uidMapping.put(localId, remoteId);
        } else {
          gidMapping.put(localId, remoteId);
        }
      }
    } finally {
      in.close();
    }
    
    return new StaticMapping(uidMapping, gidMapping);
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
      LOG.info("Can't map group " + group + ". Use its string hashcode:" + gid);
    }
    return gid;
  }
}
