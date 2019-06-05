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

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclRights;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;

import java.util.BitSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

import static org.apache.hadoop.ozone.OzoneAcl.ZERO_BITSET;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclRights.ALL;

/**
 * This helper class keeps a map of all user and their permissions.
 */
@SuppressWarnings("ProtocolBufferOrdinal")
public class OmOzoneAclMap {
  // per Acl Type user:rights map
  private ArrayList<Map<String, BitSet>> aclMaps;

  OmOzoneAclMap() {
    aclMaps = new ArrayList<>();
    for (OzoneAclType aclType : OzoneAclType.values()) {
      aclMaps.add(aclType.ordinal(), new HashMap<>());
    }
  }

  private Map<String, BitSet> getMap(OzoneAclType type) {
    return aclMaps.get(type.ordinal());
  }

  // For a given acl type and user, get the stored acl
  private BitSet getAcl(OzoneAclType type, String user) {
    return getMap(type).get(user);
  }

  public List<OzoneAcl> getAcl() {
    List<OzoneAcl> acls = new ArrayList<>();

    for (OzoneAclType type : OzoneAclType.values()) {
      aclMaps.get(type.ordinal()).entrySet().stream().
          forEach(entry -> acls.add(new OzoneAcl(ACLIdentityType.
              valueOf(type.name()), entry.getKey(), entry.getValue())));
    }
    return acls;
  }

  // Add a new acl to the map
  public void addAcl(OzoneAcl acl) throws OMException {
    Objects.requireNonNull(acl, "Acl should not be null.");
    OzoneAclType aclType = OzoneAclType.valueOf(acl.getType().name());
    if (!getMap(aclType).containsKey(acl.getName())) {
      getMap(aclType).put(acl.getName(), acl.getAclBitSet());
    } else {
      // Check if we are adding new rights to existing acl.
      BitSet temp = (BitSet) acl.getAclBitSet().clone();
      BitSet curRights = (BitSet) getMap(aclType).get(acl.getName()).clone();
      temp.or(curRights);

      if (temp.equals(curRights)) {
        // throw exception if acl is already added.
        throw new OMException("Acl " + acl + " already exist.",
            INVALID_REQUEST);
      }
      getMap(aclType).get(acl.getName()).or(acl.getAclBitSet());
    }
  }

  // Add a new acl to the map
  public void setAcls(List<OzoneAcl> acls) throws OMException {
    Objects.requireNonNull(acls, "Acls should not be null.");
    // Remove all Acls.
    for (OzoneAclType type : OzoneAclType.values()) {
      aclMaps.get(type.ordinal()).clear();
    }

    // Add acls.
    for (OzoneAcl acl : acls) {
      addAcl(acl);
    }
  }

  // Add a new acl to the map
  public void removeAcl(OzoneAcl acl) throws OMException {
    Objects.requireNonNull(acl, "Acl should not be null.");
    OzoneAclType aclType = OzoneAclType.valueOf(acl.getType().name());
    if (getMap(aclType).containsKey(acl.getName())) {
      BitSet aclRights = getMap(aclType).get(acl.getName());
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
        getMap(aclType).remove(acl.getName());
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
    if (!getMap(acl.getType()).containsKey(acl.getName())) {
      BitSet acls = new BitSet(OzoneAclRights.values().length);
      acl.getRightsList().parallelStream().forEach(a -> acls.set(a.ordinal()));
      getMap(acl.getType()).put(acl.getName(), acls);
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

    for (OzoneAclRights right : acl.getRightsList()) {
      if (aclBitSet.get(right.ordinal()) || aclBitSet.get(ALL.ordinal())) {
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
          aclMaps.get(type.ordinal()).entrySet()) {
        OzoneAclInfo.Builder builder = OzoneAclInfo.newBuilder()
            .setName(entry.getKey())
            .setType(type);
        entry.getValue().stream().forEach(a ->
            builder.addRights(OzoneAclRights.values()[a]));
        aclList.add(builder.build());
      }
    }

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
}
