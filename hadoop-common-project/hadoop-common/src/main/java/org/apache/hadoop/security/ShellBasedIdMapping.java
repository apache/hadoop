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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.Charsets;
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
 * 
 * The maps are incrementally updated as described below:
 *   1. Initialize the maps as empty. 
 *   2. Incrementally update the maps
 *      - When ShellBasedIdMapping is requested for user or group name given 
 *        an ID, or for ID given a user or group name, do look up in the map
 *        first, if it doesn't exist, find the corresponding entry with shell
 *        command, and insert the entry to the maps.
 *      - When group ID is requested for a given group name, and if the
 *        group name is numerical, the full group map is loaded. Because we
 *        don't have a good way to find the entry for a numerical group name,
 *        loading the full map helps to get in all entries.
 *   3. Periodically refresh the maps for both user and group, e.g,
 *      do step 1.
 *   Note: for testing purpose, step 1 may initial the maps with full mapping
 *   when using constructor
 *   {@link ShellBasedIdMapping#ShellBasedIdMapping(Configuration, boolean)}.
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
  private StaticMapping staticMapping = null;
  // Last time the static map was modified, measured time difference in
  // milliseconds since midnight, January 1, 1970 UTC
  private long lastModificationTimeStaticMap = 0;
  
  private boolean constructFullMapAtInit = false;

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

  /*
   * Constructor
   * @param conf the configuration
   * @param constructFullMapAtInit initialize the maps with full mapping when
   *        true, otherwise initialize the maps to empty. This parameter is
   *        intended for testing only, its default is false.
   */
  @VisibleForTesting
  public ShellBasedIdMapping(Configuration conf,
      boolean constructFullMapAtInit) throws IOException {
    this.constructFullMapAtInit = constructFullMapAtInit;
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
    
    String staticFilePath = 
        conf.get(IdMappingConstant.STATIC_ID_MAPPING_FILE_KEY,
            IdMappingConstant.STATIC_ID_MAPPING_FILE_DEFAULT);
    staticMappingFile = new File(staticFilePath);
    updateStaticMapping();
    updateMaps();
  }

  /*
   * Constructor
   * initialize user and group maps to empty
   * @param conf the configuration
   */
  public ShellBasedIdMapping(Configuration conf) throws IOException {
    this(conf, false);
  }

  @VisibleForTesting
  public long getTimeout() {
    return timeout;
  }

  @VisibleForTesting
  public BiMap<Integer, String> getUidNameMap() {
    return uidNameMap;
  }

  @VisibleForTesting
  public BiMap<Integer, String> getGidNameMap() {
    return gidNameMap;
  }

  @VisibleForTesting  
  synchronized public void clearNameMaps() {
    uidNameMap.clear();
    gidNameMap.clear();
    lastUpdateTime = Time.monotonicNow();
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
          "new entry (%d, %s), existing entry: (%d, %s).%n%s%n%s",
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
   * Get the list of users or groups returned by the specified command,
   * and save them in the corresponding map.
   * @throws IOException 
   */
  @VisibleForTesting
  public static boolean updateMapInternal(BiMap<Integer, String> map,
      String mapName, String command, String regex,
      Map<Integer, Integer> staticMapping) throws IOException  {
    boolean updated = false;
    BufferedReader br = null;
    try {
      Process process = Runtime.getRuntime().exec(
          new String[] { "bash", "-c", command });
      br = new BufferedReader(
          new InputStreamReader(process.getInputStream(),
                                Charset.defaultCharset()));
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
        updated = true;
      }
      LOG.debug("Updated " + mapName + " map size: " + map.size());
      
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
    return updated;
  }

  private boolean checkSupportedPlatform() {
    if (!OS.startsWith("Linux") && !OS.startsWith("Mac")) {
      LOG.error("Platform is not supported:" + OS
          + ". Can't update user map and group map and"
          + " 'nobody' will be used for any user and group.");
      return false;
    }
    return true;
  }

  private static boolean isInteger(final String s) {
    try { 
      Integer.parseInt(s); 
    } catch(NumberFormatException e) { 
      return false; 
    }
    // only got here if we didn't return false
    return true;
  }

  private synchronized void updateStaticMapping() throws IOException {
    final boolean init = (staticMapping == null);
    //
    // if the static mapping file
    //   - was modified after last update, load the map again;
    //   - did not exist but was added since last update, load the map;
    //   - existed before but deleted since last update, clear the map
    //
    if (staticMappingFile.exists()) {
      // check modification time, reload the file if the last modification
      // time changed since prior load.
      long lmTime = staticMappingFile.lastModified();
      if (lmTime != lastModificationTimeStaticMap) {
        LOG.info(init? "Using " : "Reloading " + "'" + staticMappingFile
            + "' for static UID/GID mapping...");
        lastModificationTimeStaticMap = lmTime;
        staticMapping = parseStaticMap(staticMappingFile);        
      }
    } else {
      if (init) {
        staticMapping = new StaticMapping(new HashMap<Integer, Integer>(),
            new HashMap<Integer, Integer>());
      }
      if (lastModificationTimeStaticMap != 0 || init) {
        // print the following log at initialization or when the static
        // mapping file was deleted after prior load
        LOG.info("Not doing static UID/GID mapping because '"
            + staticMappingFile + "' does not exist.");
      }
      lastModificationTimeStaticMap = 0;
      staticMapping.clear();
    }
  }

  /*
   * Refresh static map, and reset the other maps to empty.
   * For testing code, a full map may be re-constructed here when the object
   * was created with constructFullMapAtInit being set to true.
   */
  synchronized public void updateMaps() throws IOException {
    if (!checkSupportedPlatform()) {
      return;
    }

    if (constructFullMapAtInit) {
      loadFullMaps();
      // set constructFullMapAtInit to false to allow testing code to
      // do incremental update to maps after initial construction
      constructFullMapAtInit = false;
    } else {
      updateStaticMapping();
      clearNameMaps();
    }
  }
  
  synchronized private void loadFullUserMap() throws IOException {
    BiMap<Integer, String> uMap = HashBiMap.create();
    if (OS.startsWith("Mac")) {
      updateMapInternal(uMap, "user", MAC_GET_ALL_USERS_CMD, "\\s+",
          staticMapping.uidMapping);
    } else {
      updateMapInternal(uMap, "user", GET_ALL_USERS_CMD, ":",
          staticMapping.uidMapping);
    }
    uidNameMap = uMap;
    lastUpdateTime = Time.monotonicNow();
  }

  synchronized private void loadFullGroupMap() throws IOException {
    BiMap<Integer, String> gMap = HashBiMap.create();

    if (OS.startsWith("Mac")) {
      updateMapInternal(gMap, "group", MAC_GET_ALL_GROUPS_CMD, "\\s+",
          staticMapping.gidMapping);
    } else {
      updateMapInternal(gMap, "group", GET_ALL_GROUPS_CMD, ":",
          staticMapping.gidMapping);
    }
    gidNameMap = gMap;
    lastUpdateTime = Time.monotonicNow();
  }

  synchronized private void loadFullMaps() throws IOException {
    loadFullUserMap();
    loadFullGroupMap();
  }

  // search for id with given name, return "<name>:<id>"
  // return
  //     getent group <name> | cut -d: -f1,3
  // OR
  //     id -u <name> | awk '{print "<name>:"$1 }'
  //
  private String getName2IdCmdLinux(final String name, final boolean isGrp) {
    String cmd;
    if (isGrp) {
      cmd = "getent group " + name + " | cut -d: -f1,3";   
    } else {
      cmd = "id -u " + name + " | awk '{print \"" + name + ":\"$1 }'";
    }
    return cmd;
  }
  
  // search for name with given id, return "<name>:<id>"
  private String getId2NameCmdLinux(final int id, final boolean isGrp) {
    String cmd = "getent ";
    cmd += isGrp? "group " : "passwd ";
    cmd += String.valueOf(id) + " | cut -d: -f1,3";
    return cmd;
  }

  // "dscl . -read /Users/<name> | grep UniqueID" returns "UniqueId: <id>",
  // "dscl . -read /Groups/<name> | grep PrimaryGroupID" returns "PrimaryGoupID: <id>"
  // The following method returns a command that uses awk to process the result,
  // of these commands, and returns "<name> <id>", to simulate one entry returned by 
  // MAC_GET_ALL_USERS_CMD or MAC_GET_ALL_GROUPS_CMD.
  // Specificially, this method returns:
  // id -u <name> | awk '{print "<name>:"$1 }'
  // OR
  // dscl . -read /Groups/<name> | grep PrimaryGroupID | awk '($1 == "PrimaryGroupID:") { print "<name> " $2 }'
  //
  private String getName2IdCmdMac(final String name, final boolean isGrp) {
    String cmd;
    if (isGrp) {
      cmd = "dscl . -read /Groups/" + name;
      cmd += " | grep PrimaryGroupID | awk '($1 == \"PrimaryGroupID:\") ";
      cmd += "{ print \"" + name + "  \" $2 }'";
    } else {
      cmd = "id -u " + name + " | awk '{print \"" + name + "  \"$1 }'";
    }
    return cmd;
  }

  // "dscl . -search /Users UniqueID <id>" returns 
  //    <name> UniqueID = (
  //      <id>
  //    )
  // "dscl . -search /Groups PrimaryGroupID <id>" returns
  //    <name> PrimaryGroupID = (
  //      <id>
  //    )
  // The following method returns a command that uses sed to process the
  // the result and returns "<name> <id>" to simulate one entry returned
  // by MAC_GET_ALL_USERS_CMD or MAC_GET_ALL_GROUPS_CMD.
  // For certain negative id case like nfsnobody, the <id> is quoted as
  // "<id>", added one sed section to remove the quote.
  // Specifically, the method returns:
  // dscl . -search /Users UniqueID <id> | sed 'N;s/\\n//g;N;s/\\n//g' | sed 's/UniqueID =//g' | sed 's/)//g' | sed 's/\"//g'
  // OR
  // dscl . -search /Groups PrimaryGroupID <id> | sed 'N;s/\\n//g;N;s/\\n//g' | sed 's/PrimaryGroupID =//g' | sed 's/)//g' | sed 's/\"//g'
  //
  private String getId2NameCmdMac(final int id, final boolean isGrp) {
    String cmd = "dscl . -search /";
    cmd += isGrp? "Groups PrimaryGroupID " : "Users UniqueID ";
    cmd += String.valueOf(id);
    cmd += " | sed 'N;s/\\n//g;N;s/\\n//g' | sed 's/";
    cmd += isGrp? "PrimaryGroupID" : "UniqueID";
    cmd += " = (//g' | sed 's/)//g' | sed 's/\\\"//g'";
    return cmd;
  }

  synchronized private void updateMapIncr(final String name,
      final boolean isGrp) throws IOException {
    if (!checkSupportedPlatform()) {
      return;
    }
    if (isInteger(name) && isGrp) {
      loadFullGroupMap();
      return;
    }

    boolean updated = false;
    updateStaticMapping();

    if (OS.startsWith("Linux")) {
      if (isGrp) {
        updated = updateMapInternal(gidNameMap, "group",
            getName2IdCmdLinux(name, true), ":",
            staticMapping.gidMapping);
      } else {
        updated = updateMapInternal(uidNameMap, "user",
            getName2IdCmdLinux(name, false), ":",
            staticMapping.uidMapping);
      }
    } else {
      // Mac
      if (isGrp) {        
        updated = updateMapInternal(gidNameMap, "group",
            getName2IdCmdMac(name, true), "\\s+",
            staticMapping.gidMapping);
      } else {
        updated = updateMapInternal(uidNameMap, "user",
            getName2IdCmdMac(name, false), "\\s+",
            staticMapping.uidMapping);
      }
    }
    if (updated) {
      lastUpdateTime = Time.monotonicNow();
    }
  }

  synchronized private void updateMapIncr(final int id,
      final boolean isGrp) throws IOException {
    if (!checkSupportedPlatform()) {
      return;
    }
    
    boolean updated = false;
    updateStaticMapping();

    if (OS.startsWith("Linux")) {
      if (isGrp) {
        updated = updateMapInternal(gidNameMap, "group",
            getId2NameCmdLinux(id, true), ":",
            staticMapping.gidMapping);
      } else {
        updated = updateMapInternal(uidNameMap, "user",
            getId2NameCmdLinux(id, false), ":",
            staticMapping.uidMapping);
      }
    } else {
      // Mac
      if (isGrp) {
        updated = updateMapInternal(gidNameMap, "group",
            getId2NameCmdMac(id, true), "\\s+",
            staticMapping.gidMapping);
      } else {
        updated = updateMapInternal(uidNameMap, "user",
            getId2NameCmdMac(id, false), "\\s+",
            staticMapping.uidMapping);
      }
    }
    if (updated) {
      lastUpdateTime = Time.monotonicNow();
    }
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

    public void clear() {
      uidMapping.clear();
      gidMapping.clear();
    }

    public boolean isNonEmpty() {
      return uidMapping.size() > 0 || gidMapping.size() > 0;
    }
  }
  
  static StaticMapping parseStaticMap(File staticMapFile)
      throws IOException {
    
    Map<Integer, Integer> uidMapping = new HashMap<Integer, Integer>();
    Map<Integer, Integer> gidMapping = new HashMap<Integer, Integer>();
    
    BufferedReader in = new BufferedReader(new InputStreamReader(
        new FileInputStream(staticMapFile), Charsets.UTF_8));
    
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
        int remoteId = parseId(lineMatcher.group(2));
        int localId = parseId(lineMatcher.group(3));
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
      updateMapIncr(user, false);
      id = uidNameMap.inverse().get(user);
      if (id == null) {
        throw new IOException("User just deleted?:" + user);
      }
    }
    return id.intValue();
  }

  synchronized public int getGid(String group) throws IOException {
    checkAndUpdateMaps();

    Integer id = gidNameMap.inverse().get(group);
    if (id == null) {
      updateMapIncr(group, true);
      id = gidNameMap.inverse().get(group);
      if (id == null) {
        throw new IOException("No such group:" + group);
      }
    }
    return id.intValue();
  }

  synchronized public String getUserName(int uid, String unknown) {
    checkAndUpdateMaps();
    String uname = uidNameMap.get(uid);
    if (uname == null) {
      try {
        updateMapIncr(uid, false);
      } catch (Exception e) {        
      }
      uname = uidNameMap.get(uid);
      if (uname == null) {     
        LOG.warn("Can't find user name for uid " + uid
            + ". Use default user name " + unknown);
        uname = unknown;
      }
    }
    return uname;
  }

  synchronized public String getGroupName(int gid, String unknown) {
    checkAndUpdateMaps();
    String gname = gidNameMap.get(gid);
    if (gname == null) {
      try {
        updateMapIncr(gid, true);
      } catch (Exception e) {        
      }
      gname = gidNameMap.get(gid);
      if (gname == null) {
        LOG.warn("Can't find group name for gid " + gid
            + ". Use default group name " + unknown);
        gname = unknown;
      }
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
