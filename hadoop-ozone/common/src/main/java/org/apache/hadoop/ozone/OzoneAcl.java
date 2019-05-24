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

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

import java.util.ArrayList;
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
public class OzoneAcl {
  private ACLIdentityType type;
  private String name;
  private List<ACLType> rights;

  /**
   * Constructor for OzoneAcl.
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
    this.rights = new ArrayList<>();
    this.rights.add(acl);
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
  public OzoneAcl(ACLIdentityType type, String name, List<ACLType> acls) {
    this.name = name;
    this.rights = acls;
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
    List<ACLType> acls = new ArrayList<>();
    for (char ch : parts[2].toCharArray()) {
      acls.add(ACLType.getACLRight(String.valueOf(ch)));
    }

    // TODO : Support sanitation of these user names by calling into
    // userAuth Interface.
    return new OzoneAcl(aclType, parts[1], acls);
  }

  @Override
  public String toString() {
    return type + ":" + name + ":" + ACLType.getACLString(rights);
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
    return Objects.hash(this.getName(), this.getRights().toString(),
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
  public List<ACLType> getRights() {
    return rights;
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
    return otherAcl.toString().equals(this.toString());
  }

  /**
   * ACL types.
   */
  public enum OzoneACLType {
    USER(OzoneConsts.OZONE_ACL_USER_TYPE),
    GROUP(OzoneConsts.OZONE_ACL_GROUP_TYPE),
    WORLD(OzoneConsts.OZONE_ACL_WORLD_TYPE);

    /**
     * String value for this Enum.
     */
    private final String value;

    /**
     * Init OzoneACLtypes enum.
     *
     * @param val String type for this enum.
     */
    OzoneACLType(String val) {
      value = val;
    }
  }
}
