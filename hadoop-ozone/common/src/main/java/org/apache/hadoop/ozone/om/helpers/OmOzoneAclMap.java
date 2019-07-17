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

import com.google.protobuf.ByteString;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneAcl.ZERO_BITSET;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;

/**
 * This helper class keeps a map of all user and their permissions.
 */
@SuppressWarnings("ProtocolBufferOrdinal")
public class OmOzoneAclMap {
  // per Acl Type user:rights map
  private ArrayList<Map<String, BitSet>> accessAclMap;
  private List<OzoneAclInfo> defaultAclList;

  OmOzoneAclMap() {
    accessAclMap = new ArrayList<>();
    defaultAclList = new ArrayList<>();
    for (OzoneAclType aclType : OzoneAclType.values()) {
      accessAclMap.add(aclType.ordinal(), new HashMap<>());
    }
  }

  private Map<String, BitSet> getAccessAclMap(OzoneAclType type) {
    return accessAclMap.get(type.ordinal());
  }

  // For a given acl type and user, get the stored acl
  private BitSet getAcl(OzoneAclType type, String user) {
    return getAccessAclMap(type).get(user);
  }

  public List<OzoneAcl> getAcl() {
    List<OzoneAcl> acls = new ArrayList<>();

    acls.addAll(getAccessAcls());
    acls.addAll(defaultAclList.stream().map(a ->
        OzoneAcl.fromProtobuf(a)).collect(Collectors.toList()));
    return acls;
  }

  private Collection<? extends OzoneAcl> getAccessAcls() {
    List<OzoneAcl> acls = new ArrayList<>();
    for (OzoneAclType type : OzoneAclType.values()) {
      accessAclMap.get(type.ordinal()).entrySet().stream().
          forEach(entry -> acls.add(new OzoneAcl(ACLIdentityType.
              valueOf(type.name()), entry.getKey(), entry.getValue(),
              OzoneAcl.AclScope.ACCESS)));
    }
    return acls;
  }

  // Add a new acl to the map
  public void addAcl(OzoneAcl acl) throws OMException {
    Objects.requireNonNull(acl, "Acl should not be null.");
    if (acl.getAclScope().equals(OzoneAcl.AclScope.DEFAULT)) {
      defaultAclList.add(OzoneAcl.toProtobuf(acl));
      return;
    }

    OzoneAclType aclType = OzoneAclType.valueOf(acl.getType().name());
    if (!getAccessAclMap(aclType).containsKey(acl.getName())) {
      getAccessAclMap(aclType).put(acl.getName(), acl.getAclBitSet());
    } else {
      // Check if we are adding new rights to existing acl.
      BitSet temp = (BitSet) acl.getAclBitSet().clone();
      BitSet curRights = (BitSet) getAccessAclMap(aclType).
          get(acl.getName()).clone();
      temp.or(curRights);

      if (temp.equals(curRights)) {
        // throw exception if acl is already added.
        throw new OMException("Acl " + acl + " already exist.",
            INVALID_REQUEST);
      }
      getAccessAclMap(aclType).replace(acl.getName(), temp);
    }
  }

  // Add a new acl to the map
  public void setAcls(List<OzoneAcl> acls) throws OMException {
    Objects.requireNonNull(acls, "Acls should not be null.");
    // Remove all Acls.
    for (OzoneAclType type : OzoneAclType.values()) {
      accessAclMap.get(type.ordinal()).clear();
    }
    // Add acls.
    for (OzoneAcl acl : acls) {
      addAcl(acl);
    }
  }

  // Add a new acl to the map
  public void removeAcl(OzoneAcl acl) throws OMException {
    Objects.requireNonNull(acl, "Acl should not be null.");
    if (acl.getAclScope().equals(OzoneAcl.AclScope.DEFAULT)) {
      defaultAclList.remove(OzoneAcl.toProtobuf(acl));
      return;
    }

    OzoneAclType aclType = OzoneAclType.valueOf(acl.getType().name());
    if (getAccessAclMap(aclType).containsKey(acl.getName())) {
      BitSet aclRights = getAccessAclMap(aclType).get(acl.getName());
      BitSet bits = (BitSet) acl.getAclBitSet().clone();
      bits.and(aclRights);

      if (bits.equals(ZERO_BITSET)) {
        // throw exception if acl doesn't exist.
        throw new OMException("Acl [" + acl + "] doesn't exist.",
            INVALID_REQUEST);
      }

      acl.getAclBitSet().and(aclRights);
      aclRights.xor(acl.getAclBitSet());

      // Remove the acl as all rights are already set to 0.
      if (aclRights.equals(ZERO_BITSET)) {
        getAccessAclMap(aclType).remove(acl.getName());
      }
    } else {
      // throw exception if acl doesn't exist.
      throw new OMException("Acl [" + acl + "] doesn't exist.",
          INVALID_REQUEST);
    }
  }

  // Add a new acl to the map
  public void addAcl(OzoneAclInfo acl) throws OMException {
    Objects.requireNonNull(acl, "Acl should not be null.");
    if (acl.getAclScope().equals(OzoneAclInfo.OzoneAclScope.DEFAULT)) {
      defaultAclList.add(acl);
      return;
    }

    if (!getAccessAclMap(acl.getType()).containsKey(acl.getName())) {
      BitSet acls = BitSet.valueOf(acl.getRights().toByteArray());
      getAccessAclMap(acl.getType()).put(acl.getName(), acls);
    } else {
      // throw exception if acl is already added.

      throw new OMException("Acl " + acl + " already exist.", INVALID_REQUEST);
    }
  }

  // for a given acl, check if the user has access rights
  public boolean hasAccess(OzoneAclInfo acl) {
    if (acl == null) {
      return false;
    }

    BitSet aclBitSet = getAcl(acl.getType(), acl.getName());
    if (aclBitSet == null) {
      return false;
    }
    BitSet result = BitSet.valueOf(acl.getRights().toByteArray());
    result.and(aclBitSet);
    return (!result.equals(ZERO_BITSET) || aclBitSet.get(ALL.ordinal()))
        && !aclBitSet.get(NONE.ordinal());
  }

  /**
   * For a given acl, check if the user has access rights.
   * Acl's are checked in followoing order:
   * 1. Acls for USER.
   * 2. Acls for GROUPS.
   * 3. Acls for WORLD.
   * 4. Acls for ANONYMOUS.
   * @param acl
   * @param ugi
   *
   * @return true if given ugi has acl set, else false.
   * */
  public boolean hasAccess(ACLType acl, UserGroupInformation ugi) {
    if (acl == null) {
      return false;
    }
    if (ugi == null) {
      return false;
    }

    // Check acls in user acl list.
    return checkAccessForOzoneAclType(OzoneAclType.USER, acl, ugi)
        || checkAccessForOzoneAclType(OzoneAclType.GROUP, acl, ugi)
        || checkAccessForOzoneAclType(OzoneAclType.WORLD, acl, ugi)
        || checkAccessForOzoneAclType(OzoneAclType.ANONYMOUS, acl, ugi);
  }

  /**
   * Helper function to check acl access for OzoneAclType.
   * */
  private boolean checkAccessForOzoneAclType(OzoneAclType identityType,
      ACLType acl, UserGroupInformation ugi) {

    switch (identityType) {
    case USER:
      return OzoneUtils.checkIfAclBitIsSet(acl, getAcl(identityType,
          ugi.getUserName()));
    case GROUP:
      // Check access for user groups.
      for (String userGroup : ugi.getGroupNames()) {
        if (OzoneUtils.checkIfAclBitIsSet(acl, getAcl(identityType,
            userGroup))) {
          // Return true if any user group has required permission.
          return true;
        }
      }
      break;
    default:
      // For type WORLD and ANONYMOUS we set acl type as name.
      if(OzoneUtils.checkIfAclBitIsSet(acl, getAcl(identityType,
          identityType.name()))) {
        return true;
      }

    }
    return false;
  }

  // Convert this map to OzoneAclInfo Protobuf List
  public List<OzoneAclInfo> ozoneAclGetProtobuf() {
    List<OzoneAclInfo> aclList = new LinkedList<>();
    for (OzoneAclType type : OzoneAclType.values()) {
      for (Map.Entry<String, BitSet> entry :
          accessAclMap.get(type.ordinal()).entrySet()) {
        OzoneAclInfo.Builder builder = OzoneAclInfo.newBuilder()
            .setName(entry.getKey())
            .setType(type)
            .setAclScope(OzoneAclScope.ACCESS)
            .setRights(ByteString.copyFrom(entry.getValue().toByteArray()));

        aclList.add(builder.build());
      }
    }
    aclList.addAll(defaultAclList);
    return aclList;
  }

  // Create map from list of OzoneAclInfos
  public static OmOzoneAclMap ozoneAclGetFromProtobuf(
      List<OzoneAclInfo> aclList) throws OMException {
    OmOzoneAclMap aclMap = new OmOzoneAclMap();
    for (OzoneAclInfo acl : aclList) {
      aclMap.addAcl(acl);
    }
    return aclMap;
  }

  public Collection<? extends OzoneAcl> getAclsByScope(OzoneAclScope scope) {
    if (scope.equals(OzoneAclScope.DEFAULT)) {
      return defaultAclList.stream().map(a ->
          OzoneAcl.fromProtobuf(a)).collect(Collectors.toList());
    } else {
      return getAcl();
    }
  }

  public List<OzoneAclInfo> getDefaultAclList() {
    return defaultAclList;
  }
}
