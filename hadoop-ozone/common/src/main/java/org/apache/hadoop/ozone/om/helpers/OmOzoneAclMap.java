/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclRights;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This helper class keeps a map of all user and their permissions.
 */
@SuppressWarnings("ProtocolBufferOrdinal")
public class OmOzoneAclMap {
  // per Acl Type user:rights map
  private ArrayList<Map<String, List<OzoneAclRights>>> aclMaps;

  OmOzoneAclMap() {
    aclMaps = new ArrayList<>();
    for (OzoneAclType aclType : OzoneAclType.values()) {
      aclMaps.add(aclType.ordinal(), new HashMap<>());
    }
  }

  private Map<String, List<OzoneAclRights>> getMap(OzoneAclType type) {
    return aclMaps.get(type.ordinal());
  }

  // For a given acl type and user, get the stored acl
  private List<OzoneAclRights> getAcl(OzoneAclType type, String user) {
    return getMap(type).get(user);
  }

  // Add a new acl to the map
  public void addAcl(OzoneAclInfo acl) {
    getMap(acl.getType()).put(acl.getName(), acl.getRightsList());
  }

  // for a given acl, check if the user has access rights
  public boolean hasAccess(OzoneAclInfo acl) {
    if (acl == null) {
      return false;
    }

    List<OzoneAclRights> storedRights = getAcl(acl.getType(), acl.getName());

    for (OzoneAclRights right : storedRights) {
      switch (right) {
      case CREATE:
        return (right == OzoneAclRights.CREATE)
            || (right == OzoneAclRights.ALL);
      case LIST:
        return (right == OzoneAclRights.LIST)
            || (right == OzoneAclRights.ALL);
      case WRITE:
        return (right == OzoneAclRights.WRITE)
            || (right == OzoneAclRights.ALL);
      case READ:
        return (right == OzoneAclRights.READ)
            || (right == OzoneAclRights.ALL);
      case DELETE:
        return (right == OzoneAclRights.DELETE)
            || (right == OzoneAclRights.ALL);
      case READ_ACL:
        return (right == OzoneAclRights.READ_ACL)
            || (right == OzoneAclRights.ALL);
      case WRITE_ACL:
        return (right == OzoneAclRights.WRITE_ACL)
            || (right == OzoneAclRights.ALL);
      case ALL:
        return (right == OzoneAclRights.ALL);
      case NONE:
        return !(right == OzoneAclRights.NONE);
      default:
        return false;
      }
    }
    return false;
  }

  // Convert this map to OzoneAclInfo Protobuf List
  public List<OzoneAclInfo> ozoneAclGetProtobuf() {
    List<OzoneAclInfo> aclList = new LinkedList<>();
    for (OzoneAclType type: OzoneAclType.values()) {
      for (Map.Entry<String, List<OzoneAclRights>> entry :
          aclMaps.get(type.ordinal()).entrySet()) {
        OzoneAclInfo aclInfo = OzoneAclInfo.newBuilder()
            .setName(entry.getKey())
            .setType(type)
            .addAllRights(entry.getValue())
            .build();
        aclList.add(aclInfo);
      }
    }

    return aclList;
  }

  // Create map from list of OzoneAclInfos
  public static OmOzoneAclMap ozoneAclGetFromProtobuf(
      List<OzoneAclInfo> aclList) {
    OmOzoneAclMap aclMap = new OmOzoneAclMap();
    for (OzoneAclInfo acl : aclList) {
      aclMap.addAcl(acl);
    }
    return aclMap;
  }
}
