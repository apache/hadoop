/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.api;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.SliderProviderFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.slider.api.OptionKeys.INTERNAL_APPLICATION_HOME;
import static org.apache.slider.api.OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH;
import static org.apache.slider.api.OptionKeys.ZOOKEEPER_PATH;
import static org.apache.slider.api.OptionKeys.ZOOKEEPER_QUORUM;

/**
 * Represents a cluster specification; designed to be sendable over the wire
 * and persisted in JSON by way of Jackson.
 * 
 * When used in cluster status operations the <code>info</code>
 * and <code>statistics</code> maps contain information about the cluster.
 * 
 * As a wire format it is less efficient in both xfer and ser/deser than 
 * a binary format, but by having one unified format for wire and persistence,
 * the code paths are simplified.
 *
 * This was the original single-file specification/model used in the Hoya
 * precursor to Slider. Its now retained primarily as a way to publish
 * the current state of the application, or at least a fraction thereof ...
 * the larger set of information from the REST API is beyond the scope of
 * this structure.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)

public class ClusterDescription implements Cloneable {
  protected static final Logger
    log = LoggerFactory.getLogger(ClusterDescription.class);

  private static final String UTF_8 = "UTF-8";

  /**
   * version counter
   */
  public String version = "1.0";

  /**
   * Name of the cluster
   */
  public String name;

  /**
   * Type of cluster
   */
  public String type = SliderProviderFactory.DEFAULT_CLUSTER_TYPE;

  /**
   * State of the cluster
   */
  public int state;
  
  /*
   State list for both clusters and nodes in them. Ordered so that destroyed follows
   stopped.
   
   Some of the states are only used for recording
   the persistent state of the cluster and are not
   seen in node descriptions
   */

  /**
   * Specification is incomplete & cannot
   * be used: {@value}
   */
  public static final int STATE_INCOMPLETE = StateValues.STATE_INCOMPLETE;

  /**
   * Spec has been submitted: {@value}
   */
  public static final int STATE_SUBMITTED = StateValues.STATE_SUBMITTED;
  /**
   * Cluster created: {@value}
   */
  public static final int STATE_CREATED = StateValues.STATE_CREATED;
  /**
   * Live: {@value}
   */
  public static final int STATE_LIVE = StateValues.STATE_LIVE;
  /**
   * Stopped
   */
  public static final int STATE_STOPPED = StateValues.STATE_STOPPED;
  /**
   * destroyed
   */
  public static final int STATE_DESTROYED = StateValues.STATE_DESTROYED;
  
  /**
   * When was the cluster specification created?
   * This is not the time a cluster was thawed; that will
   * be in the <code>info</code> section.
   */
  public long createTime;

  /**
   * When was the cluster specification last updated
   */
  public long updateTime;

  /**
   * URL path to the original configuration
   * files; these are re-read when 
   * restoring a cluster
   */

  public String originConfigurationPath;

  /**
   * URL path to the generated configuration
   */
  public String generatedConfigurationPath;

  /**
   * This is where the data goes
   */
  public String dataPath;

  /**
   * cluster-specific options -to control both
   * the Slider AM and the application that it deploys
   */
  public Map<String, String> options = new HashMap<>();

  /**
   * cluster information
   * This is only valid when querying the cluster status.
   */
  public Map<String, String> info = new HashMap<>();

  /**
   * Statistics. This is only relevant when querying the cluster status
   */
  public Map<String, Map<String, Integer>> statistics = new HashMap<>();

  /**
   * Instances: role->count
   */
  public Map<String, List<String>> instances = new HashMap<>();

  /**
   * Role options, 
   * role -> option -> value
   */
  public Map<String, Map<String, String>> roles = new HashMap<>();


  /**
   * List of key-value pairs to add to a client config to set up the client
   */
  public Map<String, String> clientProperties = new HashMap<>();

  /**
   * Status information
   */
  public Map<String, Object> status;

  /**
   * Liveness information; the same as returned
   * on the <code>live/liveness/</code> URL
   */
  public ApplicationLivenessInformation liveness;

  /**
   * Creator.
   */
  public ClusterDescription() {
  }

  @Override
  public String toString() {
    try {
      return toJsonString();
    } catch (Exception e) {
      log.debug("Failed to convert CD to JSON ", e);
      return super.toString();
    }
  }

  /**
   * Shallow clone
   * @return a shallow clone
   * @throws CloneNotSupportedException
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * A deep clone of the spec. This is done inefficiently with a ser/derser
   * @return the cluster description
   */
  public ClusterDescription deepClone() {
    try {
      return fromJson(toJsonString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Save a cluster description to a hadoop filesystem
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten
   * @throws IOException IO exception
   */
  public void save(FileSystem fs, Path path, boolean overwrite) throws
                                                                IOException {
    FSDataOutputStream dataOutputStream = fs.create(path, overwrite);
    writeJsonAsBytes(dataOutputStream);
  }
  
  /**
   * Save a cluster description to the local filesystem
   * @param file file
   * @throws IOException IO excpetion
   */
  public void save(File file) throws IOException {
    log.debug("Saving to {}", file.getAbsolutePath());
    if (!file.getParentFile().mkdirs()) {
      log.warn("Failed to mkdirs for {}", file.getParentFile());
    }
    DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file));
    writeJsonAsBytes(dataOutputStream);
  }

  /**
   * Write the json as bytes -then close the file
   * @param dataOutputStream an outout stream that will always be closed
   * @throws IOException any failure
   */
  private void writeJsonAsBytes(DataOutputStream dataOutputStream)
      throws IOException {
    try {
      String json = toJsonString();
      byte[] b = json.getBytes(UTF_8);
      dataOutputStream.write(b);
    } finally {
      dataOutputStream.close();
    }
  }

  /**
   * Load from the filesystem
   * @param fs filesystem
   * @param path path
   * @return a loaded CD
   * @throws IOException IO problems
   */
  public static ClusterDescription load(FileSystem fs, Path path)
      throws IOException, JsonParseException, JsonMappingException {
    FileStatus status = fs.getFileStatus(path);
    byte[] b = new byte[(int) status.getLen()];
    FSDataInputStream dataInputStream = fs.open(path);
    int count = dataInputStream.read(b);
    String json = new String(b, 0, count, UTF_8);
    return fromJson(json);
  }

  /**
   * Make a deep copy of the class
   * @param source source
   * @return the copy
   */
  public static ClusterDescription copy(ClusterDescription source) {
    //currently the copy is done by a generate/save. Inefficient but it goes
    //down the tree nicely
    try {
      return fromJson(source.toJsonString());
    } catch (IOException e) {
      throw new RuntimeException("ClusterDescription copy failed " + e, e);
    }
  }

  /**
   * Convert to a JSON string
   * @return a JSON string description
   * @throws IOException Problems mapping/writing the object
   */
  public String toJsonString() throws IOException,
                                      JsonGenerationException,
                                      JsonMappingException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    return mapper.writeValueAsString(this);
  }

  /**
   * Convert from JSON
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public static ClusterDescription fromJson(String json)
    throws IOException, JsonParseException, JsonMappingException {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, ClusterDescription.class);
    } catch (IOException e) {
      log.error("Exception while parsing json : " + e + "\n" + json, e);
      throw e;
    }
  }

    /**
     * Convert from input stream
     * @param is input stream of cluster description
     * @return the parsed JSON
     * @throws IOException IO
     * @throws JsonMappingException failure to map from the JSON to this class
     */
    public static ClusterDescription fromStream(InputStream is)
            throws IOException, JsonParseException, JsonMappingException {
        if (is==null) {
          throw new FileNotFoundException("Empty Stream");
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(is, ClusterDescription.class);
        } catch (IOException e) {
            log.error("Exception while parsing input stream : {}", e, e);
      throw e;
    }
  }

  /**
   * Convert from a JSON file
   * @param jsonFile input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public static ClusterDescription fromFile(File jsonFile)
    throws IOException, JsonParseException, JsonMappingException {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(jsonFile, ClusterDescription.class);
    } catch (IOException e) {
      log.error("Exception while parsing json file {}" , jsonFile, e);
      throw e;
    }
  }

  /**
   * Set a cluster option: a key val pair in the options {} section
   * @param key key option name
   * @param val value option value
   */
  public void setOption(String key, String val) {
    options.put(key, val);
  }

  /**
   * Set a cluster option if it is unset. If it is already set,
   * in the Cluster Description, it is left alone
   * @param key key key to query/set
   * @param val value value
   */

  public void setOptionifUnset(String key, String val) {
    if (options.get(key) == null) {
      options.put(key, val);
    }
  }

  /**
   * Set an integer option -it's converted to a string before saving
   * @param option option name
   * @param val integer value
   */
  public void setOption(String option, int val) {
    setOption(option, Integer.toString(val));
  }

  /**
   * Set a boolean option
   * @param option option name
   * @param val bool value
   */
  public void setOption(String option, boolean val) {
    setOption(option, Boolean.toString(val));
  }

  /**
   * Get a cluster option or value
   *
   * @param key option key
   * @param defVal option val
   * @return resolved value or default
   */
  public String getOption(String key, String defVal) {
    String val = options.get(key);
    return val != null ? val : defVal;
  }

  /**
   * Get a cluster option or value
   *
   * @param key mandatory key
   * @return the value
   * @throws BadConfigException if the option is missing
   */
  public String getMandatoryOption(String key) throws BadConfigException {
    String val = options.get(key);
    if (val == null) {
      throw new BadConfigException("Missing option " + key);
    }
    return val;
  }

  /**
   * Get an integer option; use {@link Integer#decode(String)} so as to take hex
   * oct and bin values too.
   *
   * @param option option name
   * @param defVal default value
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public int getOptionInt(String option, int defVal) {
    String val = getOption(option, Integer.toString(defVal));
    return Integer.decode(val);
  }

  /**
   * Verify that an option is set: that is defined AND non-empty
   * @param key key to verify
   * @throws BadConfigException
   */
  public void verifyOptionSet(String key) throws BadConfigException {
    if (SliderUtils.isUnset(getOption(key, null))) {
      throw new BadConfigException("Unset cluster option %s", key);
    }
  }

  /**
   * Get an option as a boolean. Note that {@link Boolean#valueOf(String)}
   * is used for parsing -its policy of what is true vs false applies.
   * @param option name
   * @param defVal default
   * @return the option.
   */
  public boolean getOptionBool(String option, boolean defVal) {
    return Boolean.valueOf(getOption(option, Boolean.toString(defVal)));
  }

  /**
   * Get a role option
   * @param role role to get from
   * @param option option name
   * @param defVal default value
   * @return resolved value
   */
  public String getRoleOpt(String role, String option, String defVal) {
    Map<String, String> roleopts = getRole(role);
    if (roleopts == null) {
      return defVal;
    }
    String val = roleopts.get(option);
    return val != null ? val : defVal;
  }

  /**
   * Get a mandatory role option
   * @param role role to get from
   * @param option option name
   * @return resolved value
   * @throws BadConfigException if the option is not defined
   */
  public String getMandatoryRoleOpt(String role, String option) throws
                                                                BadConfigException {
    Map<String, String> roleopts = getRole(role);
    if (roleopts == null) {
      throw new BadConfigException("Missing role %s ", role);
    }
    String val = roleopts.get(option);
    if (val == null) {
      throw new BadConfigException("Missing option '%s' in role %s ", option, role);
    }
    return val;
  }

  /**
   * Get a mandatory integer role option
   * @param role role to get from
   * @param option option name
   * @return resolved value
   * @throws BadConfigException if the option is not defined
   */
  public int getMandatoryRoleOptInt(String role, String option)
      throws BadConfigException {
    getMandatoryRoleOpt(role, option);
    return getRoleOptInt(role, option, 0);
  }
  
  /**
   * look up a role and return its options
   * @param role role
   * @return role mapping or null
   */
  public Map<String, String> getRole(String role) {
    return roles.get(role);
  }

  /**
   * Get a role -adding it to the roleopts map if
   * none with that name exists
   * @param role role
   * @return role mapping
   */
  public Map<String, String> getOrAddRole(String role) {
    Map<String, String> map = getRole(role);
    if (map == null) {
      map = new HashMap<>();
    }
    roles.put(role, map);
    return map;
  }
  
  /*
   * return the Set of role names
   */
  @JsonIgnore
  public Set<String> getRoleNames() {
    return new HashSet<>(roles.keySet());
  }

  /**
   * Get a role whose presence is mandatory
   * @param role role name
   * @return the mapping
   * @throws BadConfigException if the role is not there
   */
  public Map<String, String> getMandatoryRole(String role) throws
                                                           BadConfigException {
    Map<String, String> roleOptions = getRole(role);
    if (roleOptions == null) {
      throw new BadConfigException("Missing role " + role);
    }
    return roleOptions;
  }

  /**
   * Get an integer role option; use {@link Integer#decode(String)} so as to take hex
   * oct and bin values too.
   *
   * @param role role to get from
   * @param option option name
   * @param defVal default value
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public int getRoleOptInt(String role, String option, int defVal) {
    String val = getRoleOpt(role, option, Integer.toString(defVal));
    return Integer.decode(val);
  }

  /**
   * Get an integer role option; use {@link Integer#decode(String)} so as to take hex
   * oct and bin values too.
   *
   * @param role role to get from
   * @param option option name
   * @param defVal default value
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public long getRoleOptLong(String role, String option, long defVal) {
    String val = getRoleOpt(role, option, Long.toString(defVal));
    return Long.decode(val);
  }

  /**
   * Set a role option, creating the role if necessary
   * @param role role name
   * @param option option name
   * @param val value
   */
  public void setRoleOpt(String role, String option, String val) {
    Map<String, String> roleopts = getOrAddRole(role);
    roleopts.put(option, val);
  }

  /**
   * Set an integer role option, creating the role if necessary
   * @param role role name
   * @param option option name
   * @param val integer value
   */
  public void setRoleOpt(String role, String option, int val) {
    setRoleOpt(role, option, Integer.toString(val));
  }

  /**
   * Set a role option of any object, using its string value.
   * This works for (Boxed) numeric values as well as other objects
   * @param role role name
   * @param option option name
   * @param val non-null value
   */
  public void setRoleOpt(String role, String option, Object val) {
    setRoleOpt(role, option, val.toString());
  }

  /**
   * Get the value of a role requirement (cores, RAM, etc).
   * These are returned as integers, but there is special handling of the 
   * string {@link ResourceKeys#YARN_RESOURCE_MAX}, which triggers
   * the return of the maximum value.
   * @param role role to get from
   * @param option option name
   * @param defVal default value
   * @param maxVal value to return if the max val is requested
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public int getRoleResourceRequirement(String role, String option, int defVal, int maxVal) {
    String val = getRoleOpt(role, option, Integer.toString(defVal));
    Integer intVal;
    if (ResourceKeys.YARN_RESOURCE_MAX.equals(val)) {
      intVal = maxVal;
    } else {
      intVal = Integer.decode(val);
    }
    return intVal;
  }


  /**
   * Set the time for an information (human, machine) timestamp pair of fields.
   * The human time is the time in millis converted via the {@link Date} class.
   * @param keyHumanTime name of human time key
   * @param keyMachineTime name of machine time
   * @param time timestamp
   */
  
  public void setInfoTime(String keyHumanTime, String keyMachineTime, long time) {
    SliderUtils.setInfoTime(info, keyHumanTime, keyMachineTime, time);
  }

  /**
   * Set an information string. This is content that is only valid in status
   * reports.
   * @param key key
   * @param value string value
   */
  @JsonIgnore
  public void setInfo(String key, String value) {
    info.put(key, value);
  }

  /**
   * Get an information string. This is content that is only valid in status
   * reports.
   * @param key key
   * @return the value or null
   */
  @JsonIgnore
  public String getInfo(String key) {
    return info.get(key);
  }

  /**
   * Get an information string. This is content that is only valid in status
   * reports.
   * @param key key
   * @return the value or null
   */
  @JsonIgnore
  public boolean getInfoBool(String key) {
    String val = info.get(key);
    if (val != null) {
      return Boolean.valueOf(val);
    }
    return false;
  }

  @JsonIgnore
  public String getZkHosts() throws BadConfigException {
    return getMandatoryOption(ZOOKEEPER_QUORUM);
  }

  /**
   * Set the hosts for the ZK quorum
   * @param zkHosts a comma separated list of hosts
   */
  @JsonIgnore
  public void setZkHosts(String zkHosts) {
    setOption(ZOOKEEPER_QUORUM, zkHosts);
  }

  @JsonIgnore
  public String getZkPath() throws BadConfigException {
    return getMandatoryOption(ZOOKEEPER_PATH);
  }

  @JsonIgnore
  public void setZkPath(String zkPath) {
    setOption(ZOOKEEPER_PATH, zkPath);
  }

  /**
   * HBase home: if non-empty defines where a copy of HBase is preinstalled
   */
  @JsonIgnore
  public String getApplicationHome() {
    return getOption(INTERNAL_APPLICATION_HOME, "");
  }

  @JsonIgnore
  public void setApplicationHome(String applicationHome) {
    setOption(INTERNAL_APPLICATION_HOME, applicationHome);
  }

  /**
   * The path in HDFS where the HBase image is
   */
  @JsonIgnore
  public String getImagePath() {
    return getOption(INTERNAL_APPLICATION_IMAGE_PATH, "");
  }

  /**
   * Set the path in HDFS where the HBase image is
   */
  @JsonIgnore
  public void setImagePath(String imagePath) {
    setOption(INTERNAL_APPLICATION_IMAGE_PATH, imagePath);
  }

  /**
   * Query for the image path being set (non null/non empty)
   * @return true if there is a path in the image path option
   */
  @JsonIgnore
  public boolean isImagePathSet() {
    return SliderUtils.isSet(getImagePath());
  }
}
