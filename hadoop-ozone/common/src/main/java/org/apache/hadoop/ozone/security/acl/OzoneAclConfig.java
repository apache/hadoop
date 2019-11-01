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
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

/**
 * Ozone ACL config pojo.
 * */
@ConfigGroup(prefix = "ozone.om")
public class OzoneAclConfig {
  // OM Default user/group permissions
  private ACLType userDefaultRights = ACLType.ALL;
  private ACLType groupDefaultRights = ACLType.ALL;

  @Config(key = "user.rights",
      defaultValue = "ALL",
      type = ConfigType.STRING,
      tags = {ConfigTag.OM, ConfigTag.SECURITY},
      description = "Default user permissions set for an object in " +
          "OzoneManager."
  )
  public void setUserDefaultRights(String userRights) {
    if(userRights == null) {
      userRights = "ALL";
    }
    this.userDefaultRights = ACLType.valueOf(userRights);
  }

  @Config(key = "group.rights",
      defaultValue = "ALL",
      type = ConfigType.STRING,
      tags = {ConfigTag.OM, ConfigTag.SECURITY},
      description = "Default group permissions set for an object in " +
          "OzoneManager."
  )
  public void setGroupDefaultRights(String groupRights) {
    if(groupRights == null) {
      groupRights = "ALL";
    }
    this.groupDefaultRights = ACLType.valueOf(groupRights);
  }

  public ACLType getUserDefaultRights() {
    return userDefaultRights;
  }

  public ACLType getGroupDefaultRights() {
    return groupDefaultRights;
  }

}
