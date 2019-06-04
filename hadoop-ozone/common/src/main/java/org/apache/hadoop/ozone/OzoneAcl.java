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


package org.apache.hadoop.ozone;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclRights;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * OzoneACL classes define bucket ACLs used in OZONE.
 *
 * ACLs in Ozone follow this pattern.
 * <ul>
 * <li>user:name:rw
 * <li>group:name:rw
 * <li>world::rw
 * </ul>
 */
@JsonIgnoreProperties(value = {"aclBitSet"})
public class OzoneAcl {
  private ACLIdentityType type;
  private String name;
  private BitSet aclBitSet;
  public static final BitSet ZERO_BITSET = new BitSet(0);

  /**
   * Default constructor.
   */
  public OzoneAcl() {
  }

  /**
   * Constructor for OzoneAcl.
   *
   * @param type - Type
   * @param name - Name of user
   * @param acl - Rights
   */
  public OzoneAcl(ACLIdentityType type, String name, ACLType acl) {
    this.name = name;
    this.aclBitSet = new BitSet(ACLType.getNoOfAcls());
    aclBitSet.set(acl.ordinal(), true);
    this.type = type;
    if (type == ACLIdentityType.WORLD && name.length() != 0) {
      throw new IllegalArgumentException("Unexpected name part in world type");
    }
    if (((type == ACLIdentityType.USER) || (type == ACLIdentityType.GROUP))
        && (name.length() == 0)) {
      throw new IllegalArgumentException("User or group name is required");
    }
  }

  /**
   * Constructor for OzoneAcl.
   *
   * @param type - Type
   * @param name - Name of user
   * @param acls - Rights
   */
  public OzoneAcl(ACLIdentityType type, String name, BitSet acls) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(acls);

    if(acls.cardinality() > ACLType.getNoOfAcls()) {
      throw new IllegalArgumentException("Acl bitset passed has unexpected " +
          "size. bitset size:" + acls.cardinality() + ", bitset:"
          + acls.toString());
    }

    this.aclBitSet = (BitSet) acls.clone();
    acls.stream().forEach(a -> aclBitSet.set(a));

    this.name = name;
    this.type = type;
    if (type == ACLIdentityType.WORLD && name.length() != 0) {
      throw new IllegalArgumentException("Unexpected name part in world type");
    }
    if (((type == ACLIdentityType.USER) || (type == ACLIdentityType.GROUP))
        && (name.length() == 0)) {
      throw new IllegalArgumentException("User or group name is required");
    }
  }

  /**
   * Parses an ACL string and returns the ACL object.
   *
   * @param acl - Acl String , Ex. user:anu:rw
   *
   * @return - Ozone ACLs
   */
  public static OzoneAcl parseAcl(String acl) throws IllegalArgumentException {
    if ((acl == null) || acl.isEmpty()) {
      throw new IllegalArgumentException("ACLs cannot be null or empty");
    }
    String[] parts = acl.trim().split(":");
    if (parts.length < 3) {
      throw new IllegalArgumentException("ACLs are not in expected format");
    }

    ACLIdentityType aclType = ACLIdentityType.valueOf(parts[0].toUpperCase());
    BitSet acls = new BitSet(ACLType.getNoOfAcls());

    for (char ch : parts[2].toCharArray()) {
      acls.set(ACLType.getACLRight(String.valueOf(ch)).ordinal());
    }

    // TODO : Support sanitation of these user names by calling into
    // userAuth Interface.
    return new OzoneAcl(aclType, parts[1], acls);
  }

  public static OzoneAclInfo toProtobuf(OzoneAcl acl) {
    OzoneAclInfo.Builder builder = OzoneAclInfo.newBuilder()
        .setName(acl.getName())
        .setType(OzoneAclType.valueOf(acl.getType().name()));
    acl.getAclBitSet().stream().forEach(a ->
        builder.addRights(OzoneAclRights.valueOf(ACLType.values()[a].name())));
    return builder.build();
  }

  public static OzoneAcl fromProtobuf(OzoneAclInfo protoAcl) {
    BitSet aclRights = new BitSet(ACLType.getNoOfAcls());
    protoAcl.getRightsList().parallelStream().forEach(a ->
        aclRights.set(a.ordinal()));

    return new OzoneAcl(ACLIdentityType.valueOf(protoAcl.getType().name()),
        protoAcl.getName(), aclRights);
  }

  @Override
  public String toString() {
    return type + ":" + name + ":" + ACLType.getACLString(aclBitSet);
  }

  /**
   * Returns a hash code value for the object. This method is
   * supported for the benefit of hash tables.
   *
   * @return a hash code value for this object.
   *
   * @see Object#equals(Object)
   * @see System#identityHashCode
   */
  @Override
  public int hashCode() {
    return Objects.hash(this.getName(), this.getAclBitSet(),
                        this.getType().toString());
  }

  /**
   * Returns name.
   *
   * @return name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns Rights.
   *
   * @return - Rights
   */
  public BitSet getAclBitSet() {
    return aclBitSet;
  }

  public List<ACLType> getAclList() {
    List<ACLType> acls = new ArrayList<>(ACLType.getNoOfAcls());
    if(aclBitSet !=  null) {
      aclBitSet.stream().forEach(a -> acls.add(ACLType.values()[a]));
    }
    return acls;
  }

  /**
   * Returns Type.
   *
   * @return type
   */
  public ACLIdentityType getType() {
    return type;
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param obj the reference object with which to compare.
   *
   * @return {@code true} if this object is the same as the obj
   * argument; {@code false} otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    OzoneAcl otherAcl = (OzoneAcl) obj;
    return otherAcl.getName().equals(this.getName()) &&
        otherAcl.getType().equals(this.getType()) &&
        otherAcl.getAclBitSet().equals(this.getAclBitSet());
  }
}
