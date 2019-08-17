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
import com.google.protobuf.ByteString;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

  private static final String ACL_SCOPE_REGEX = ".*\\[(ACCESS|DEFAULT)\\]";
  private ACLIdentityType type;
  private String name;
  private BitSet aclBitSet;
  private AclScope aclScope;
  private static final List<ACLType> EMPTY_LIST = new ArrayList<>(0);
  public static final BitSet ZERO_BITSET = new BitSet(0);

  /**
   * Default constructor.
   */
  public OzoneAcl() {
  }

  /**
   * Constructor for OzoneAcl.
   *
   * @param type   - Type
   * @param name   - Name of user
   * @param acl    - Rights
   * @param scope  - AclScope
   */
  public OzoneAcl(ACLIdentityType type, String name, ACLType acl,
      AclScope scope) {
    this.name = name;
    this.aclBitSet = new BitSet(ACLType.getNoOfAcls());
    aclBitSet.set(acl.ordinal(), true);
    this.type = type;
    if (type == ACLIdentityType.WORLD || type == ACLIdentityType.ANONYMOUS) {
      if (!name.equals(ACLIdentityType.WORLD.name()) &&
          !name.equals(ACLIdentityType.ANONYMOUS.name()) &&
          name.length() != 0) {
        throw new IllegalArgumentException("Unexpected name:{" + name +
            "} for type WORLD, ANONYMOUS. It should be WORLD & " +
            "ANONYMOUS respectively.");
      }
      // For type WORLD and ANONYMOUS we allow only one acl to be set.
      this.name = type.name();
    }
    if (((type == ACLIdentityType.USER) || (type == ACLIdentityType.GROUP))
        && (name.length() == 0)) {
      throw new IllegalArgumentException("User or group name is required");
    }
    aclScope = scope;
  }

  /**
   * Constructor for OzoneAcl.
   *
   * @param type   - Type
   * @param name   - Name of user
   * @param acls   - Rights
   * @param scope  - AclScope
   */
  public OzoneAcl(ACLIdentityType type, String name, BitSet acls,
      AclScope scope) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(acls);

    if(acls.cardinality() > ACLType.getNoOfAcls()) {
      throw new IllegalArgumentException("Acl bitset passed has unexpected " +
          "size. bitset size:" + acls.cardinality() + ", bitset:"
          + acls.toString());
    }
    this.aclBitSet = (BitSet) acls.clone();

    this.name = name;
    this.type = type;
    if (type == ACLIdentityType.WORLD || type == ACLIdentityType.ANONYMOUS) {
      if (!name.equals(ACLIdentityType.WORLD.name()) &&
          !name.equals(ACLIdentityType.ANONYMOUS.name()) &&
          name.length() != 0) {
        throw new IllegalArgumentException("Unexpected name:{" + name +
            "} for type WORLD, ANONYMOUS. It should be WORLD & " +
            "ANONYMOUS respectively.");
      }
      // For type WORLD and ANONYMOUS we allow only one acl to be set.
      this.name = type.name();
    }
    if (((type == ACLIdentityType.USER) || (type == ACLIdentityType.GROUP))
        && (name.length() == 0)) {
      throw new IllegalArgumentException("User or group name is required");
    }
    aclScope = scope;
  }

  /**
   * Parses an ACL string and returns the ACL object. If acl scope is not
   * passed in input string then scope is set to ACCESS.
   *
   * @param acl - Acl String , Ex. user:anu:rw
   *
   * @return - Ozone ACLs
   */
  public static OzoneAcl parseAcl(String acl)
      throws IllegalArgumentException {
    if ((acl == null) || acl.isEmpty()) {
      throw new IllegalArgumentException("ACLs cannot be null or empty");
    }
    String[] parts = acl.trim().split(":");
    if (parts.length < 3) {
      throw new IllegalArgumentException("ACLs are not in expected format");
    }

    ACLIdentityType aclType = ACLIdentityType.valueOf(parts[0].toUpperCase());
    BitSet acls = new BitSet(ACLType.getNoOfAcls());

    String bits = parts[2];

    // Default acl scope is ACCESS.
    AclScope aclScope = AclScope.ACCESS;

    // Check if acl string contains scope info.
    if(parts[2].matches(ACL_SCOPE_REGEX)) {
      int indexOfOpenBracket = parts[2].indexOf("[");
      bits = parts[2].substring(0, indexOfOpenBracket);
      aclScope = AclScope.valueOf(parts[2].substring(indexOfOpenBracket + 1,
          parts[2].indexOf("]")));
    }

    // Set all acl bits.
    for (char ch : bits.toCharArray()) {
      acls.set(ACLType.getACLRight(String.valueOf(ch)).ordinal());
    }

    // TODO : Support sanitation of these user names by calling into
    // userAuth Interface.
    return new OzoneAcl(aclType, parts[1], acls, aclScope);
  }

  /**
   * Parses an ACL string and returns the ACL object.
   *
   * @param acls - Acl String , Ex. user:anu:rw
   *
   * @return - Ozone ACLs
   */
  public static List<OzoneAcl> parseAcls(String acls)
      throws IllegalArgumentException {
    if ((acls == null) || acls.isEmpty()) {
      throw new IllegalArgumentException("ACLs cannot be null or empty");
    }
    String[] parts = acls.trim().split(",");
    if (parts.length < 1) {
      throw new IllegalArgumentException("ACLs are not in expected format");
    }
    List<OzoneAcl> ozAcls = new ArrayList<>();

    for(String acl:parts) {
      ozAcls.add(parseAcl(acl));
    }
    return ozAcls;
  }

  public static OzoneAclInfo toProtobuf(OzoneAcl acl) {
    OzoneAclInfo.Builder builder = OzoneAclInfo.newBuilder()
        .setName(acl.getName())
        .setType(OzoneAclType.valueOf(acl.getType().name()))
        .setAclScope(OzoneAclScope.valueOf(acl.getAclScope().name()))
        .setRights(ByteString.copyFrom(acl.getAclBitSet().toByteArray()));
    return builder.build();
  }

  public static OzoneAcl fromProtobuf(OzoneAclInfo protoAcl) {
    BitSet aclRights = BitSet.valueOf(protoAcl.getRights().toByteArray());
    return new OzoneAcl(ACLIdentityType.valueOf(protoAcl.getType().name()),
        protoAcl.getName(), aclRights,
        AclScope.valueOf(protoAcl.getAclScope().name()));
  }

  /**
   * Helper function to convert a proto message of type {@link OzoneAclInfo}
   * to {@link OzoneAcl} with acl scope of type ACCESS.
   *
   * @param protoAcl
   * @return OzoneAcl
   * */
  public static OzoneAcl fromProtobufWithAccessType(OzoneAclInfo protoAcl) {
    BitSet aclRights = BitSet.valueOf(protoAcl.getRights().toByteArray());
    return new OzoneAcl(ACLIdentityType.valueOf(protoAcl.getType().name()),
        protoAcl.getName(), aclRights, AclScope.ACCESS);
  }

  /**
   * Helper function to convert an {@link OzoneAcl} to proto message of type
   * {@link OzoneAclInfo} with acl scope of type ACCESS.
   *
   * @param acl
   * @return OzoneAclInfo
   * */
  public static OzoneAclInfo toProtobufWithAccessType(OzoneAcl acl) {
    OzoneAclInfo.Builder builder = OzoneAclInfo.newBuilder()
        .setName(acl.getName())
        .setType(OzoneAclType.valueOf(acl.getType().name()))
        .setAclScope(OzoneAclScope.ACCESS)
        .setRights(ByteString.copyFrom(acl.getAclBitSet().toByteArray()));
    return builder.build();
  }

  public AclScope getAclScope() {
    return aclScope;
  }

  @Override
  public String toString() {
    return type + ":" + name + ":" + ACLType.getACLString(aclBitSet)
        + "[" + aclScope + "]";
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
                        this.getType().toString(), this.getAclScope());
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
    if(aclBitSet !=  null) {
      return aclBitSet.stream().mapToObj(a ->
          ACLType.values()[a]).collect(Collectors.toList());
    }
    return EMPTY_LIST;
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
        otherAcl.getAclBitSet().equals(this.getAclBitSet()) &&
        otherAcl.getAclScope().equals(this.getAclScope());
  }

  /**
   * Scope of ozone acl.
   * */
  public enum AclScope {
    ACCESS,
    DEFAULT;
  }
}
