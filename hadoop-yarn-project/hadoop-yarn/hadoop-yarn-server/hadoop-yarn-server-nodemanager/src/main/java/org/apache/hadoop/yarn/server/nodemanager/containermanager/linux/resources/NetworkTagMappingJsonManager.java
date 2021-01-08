/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * The NetworkTagMapping JsonManager implementation.
 */
public class NetworkTagMappingJsonManager implements NetworkTagMappingManager {

  /** Format of the classid that is to be used with the net_cls cgroup. Needs
   * to be of the form 0xAAAABBBB */
  private static final String FORMAT_NET_CLS_CLASS_ID = "0x[0-9]{8}";

  private NetworkTagMapping networkTagMapping = null;

  @Override
  public void initialize(Configuration conf) {
    String mappingJsonFile = conf.get(
        YarnConfiguration.NM_NETWORK_TAG_MAPPING_FILE_PATH,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_TAG_MAPPING_FILE_PATH);
    if (mappingJsonFile == null || mappingJsonFile.isEmpty()) {
      throw new YarnRuntimeException("To use NetworkTagMappingJsonManager,"
          + " we have to set the configuration:" +
          YarnConfiguration.NM_NETWORK_TAG_MAPPING_FILE_PATH);
    }
    ObjectMapper mapper = new ObjectMapper();
    try {
      networkTagMapping = mapper.readValue(new File(mappingJsonFile),
          NetworkTagMapping.class);
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }

    if (networkTagMapping == null) {
      throw new YarnRuntimeException("Fail to load the specific JSON file: "
          + mappingJsonFile);
    }

    networkTagMapping.validateUsers();
    networkTagMapping.validateGroups();
    networkTagMapping.validateDefaultClass();
  }

  @Override
  public String getNetworkTagHexID(Container container) {
    String userNetworkTagID = this.networkTagMapping.getUserNetworkTagID(
        container.getUser());
    if (userNetworkTagID != null) {
      return userNetworkTagID;
    }

    UserGroupInformation userUGI = UserGroupInformation.createRemoteUser(
        container.getUser());
    List<Group> groups = this.networkTagMapping.getGroups();
    for(Group group : groups) {
      if (userUGI.getGroups().contains(group.getGroupName())) {
        return group.getNetworkTagID();
      }
    }

    return this.networkTagMapping.getDefaultNetworkTagID();
  }

  /**
   * The NetworkTagMapping object.
   *
   */
  @VisibleForTesting
  @Private
  public static class NetworkTagMapping {
    @JsonProperty("users")
    private List<User> users = new LinkedList<>();
    @JsonProperty("groups")
    private List<Group> groups = new LinkedList<>();
    @JsonProperty("default-network-tag-id")
    private String defaultNetworkTagID;
    @JsonIgnore
    private final Pattern pattern = Pattern.compile(FORMAT_NET_CLS_CLASS_ID);

    public NetworkTagMapping() {}

    public List<User> getUsers() {
      return this.users;
    }

    public void setUsers(List<User> users) {
      this.users = users;
    }

    public void addUser(User user) {
      this.users.add(user);
    }

    public String getUserNetworkTagID(String userName) {
      for (User user : users) {
        if (userName.equals(user.getUserName())) {
          return user.getNetworkTagID();
        }
      }
      return null;
    }

    public List<Group> getGroups() {
      return this.groups;
    }

    public void setGroups(List<Group> groups) {
      this.groups = groups;
    }

    public void addGroup(Group group) {
      this.groups.add(group);
    }

    public String getDefaultNetworkTagID() {
      return this.defaultNetworkTagID;
    }

    public void setDefaultNetworkTagID(String defaultNetworkTagID) {
      this.defaultNetworkTagID = defaultNetworkTagID;
    }

    private boolean containsUser(String user, List<User> userList) {
      for (User existing : userList) {
        if (user.equals(existing.getUserName())) {
          return true;
        }
      }
      return false;
    }

    private boolean containsGroup(String group, List<Group> groupList) {
      for (Group existing : groupList) {
        if (group.equals(existing.getGroupName())) {
          return true;
        }
      }
      return false;
    }

    // Make sure that we do not have the duplicate user names.
    // If it exists, we would only keep the user name which is
    // set first.
    // Also, make sure the class_id set for the user match the
    // 0xAAAABBBB format
    public void validateUsers() {
      List<User> validateUsers = new LinkedList<>();
      for(User user : this.users) {
        Matcher m = pattern.matcher(user.getNetworkTagID());
        if (!m.matches()) {
          throw new YarnRuntimeException(
              "User-network-tag-id mapping configuraton error. "
              + "The user:" + user.getUserName()
              + " 's configured network-tag-id:" + user.getNetworkTagID()
              + " does not match the '0xAAAABBBB' format.");
        }
        String[] userSplits = user.getUserName().split(",");
        if (userSplits.length > 1) {
          String networkTagID = user.getNetworkTagID();
          for(String split : userSplits) {
            if (!containsUser(split.trim(), validateUsers)) {
              User addUsers = new User(split.trim(), networkTagID);
              validateUsers.add(addUsers);
            }
          }
        } else {
          if (!containsUser(user.getUserName(), validateUsers)) {
            validateUsers.add(user);
          }
        }
      }
      this.users = validateUsers;
    }

    // Make sure that we do not have the duplicate group names.
    // If it exists, we would only keep the group name which is
    // set first.
    // Also, make sure the class_id set for the group match the
    // 0xAAAABBBB format
    public void validateGroups() {
      List<Group> validateGroups = new LinkedList<>();
      for(Group group : this.groups) {
        if (!containsGroup(group.getGroupName(), validateGroups)) {
          Matcher m = pattern.matcher(group.getNetworkTagID());
          if (!m.matches()) {
            throw new YarnRuntimeException(
                "Group-network-tag-id mapping configuraton error. "
                + "The group:" + group.getGroupName()
                + " 's configured network-tag-id:" + group.getNetworkTagID()
                + " does not match the '0xAAAABBBB' format.");
          }
          validateGroups.add(group);
        }
      }
      this.groups = validateGroups;
    }

    // make sure that we set the value for default-network-tag-id.
    // Also, make sure the default class id match the
    // 0xAAAABBBB format
    public void validateDefaultClass() {
      if (getDefaultNetworkTagID() == null ||
          getDefaultNetworkTagID().isEmpty()) {
        throw new YarnRuntimeException("Missing value for defaultNetworkTagID."
            + " We have to set non-empty value for defaultNetworkTagID");
      }
      Matcher m = pattern.matcher(getDefaultNetworkTagID());
      if (!m.matches()) {
        throw new YarnRuntimeException("Configuration error on "
            + "default-network-tag-id. The configured default-network-tag-id:"
            + getDefaultNetworkTagID()
            + " does not match the '0xAAAABBBB' format.");
      }
    }
  }

  /**
   * The user object.
   *
   */
  @VisibleForTesting
  @Private
  public static class User {
    @JsonProperty("name")
    private String userName;
    @JsonProperty("network-tag-id")
    private String networkTagID;

    public User() {}

    public User(String userName, String networkTagID) {
      this.setUserName(userName);
      this.setNetworkTagID(networkTagID);
    }

    public String getUserName() {
      return userName;
    }
    public void setUserName(String userName) {
      this.userName = userName;
    }
    public String getNetworkTagID() {
      return networkTagID;
    }
    public void setNetworkTagID(String networkTagID) {
      this.networkTagID = networkTagID;
    }
  }

  /**
   * The group object.
   *
   */
  @VisibleForTesting
  @Private
  public static class Group {
    @JsonProperty("name")
    private String groupName;
    @JsonProperty("network-tag-id")
    private String networkTagID;

    public Group() {}

    public String getGroupName() {
      return groupName;
    }
    public void setGroupName(String groupName) {
      this.groupName = groupName;
    }

    public String getNetworkTagID() {
      return networkTagID;
    }
    public void setNetworkTagID(String networkTagID) {
      this.networkTagID = networkTagID;
    }
  }

  @Private
  @VisibleForTesting
  public NetworkTagMapping getNetworkTagMapping() {
    return this.networkTagMapping;
  }
}
