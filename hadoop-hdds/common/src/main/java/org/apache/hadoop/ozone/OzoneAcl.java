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
  private OzoneACLType type;
  private String name;
  private OzoneACLRights rights;

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
   * @param rights - Rights
   */
  public OzoneAcl(OzoneACLType type, String name, OzoneACLRights rights) {
    this.name = name;
    this.rights = rights;
    this.type = type;
    if (type == OzoneACLType.WORLD && name.length() != 0) {
      throw new IllegalArgumentException("Unexpected name part in world type");
    }
    if (((type == OzoneACLType.USER) || (type == OzoneACLType.GROUP))
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

    OzoneACLType aclType = OzoneACLType.valueOf(parts[0].toUpperCase());
    OzoneACLRights rights = OzoneACLRights.getACLRight(parts[2].toLowerCase());

    // TODO : Support sanitation of these user names by calling into
    // userAuth Interface.
    return new OzoneAcl(aclType, parts[1], rights);
  }

  @Override
  public String toString() {
    return type + ":" + name + ":" + OzoneACLRights.getACLRightsString(rights);
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
  public OzoneACLRights getRights() {
    return rights;
  }

  /**
   * Returns Type.
   *
   * @return type
   */
  public OzoneACLType getType() {
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
        otherAcl.getRights() == this.getRights() &&
        otherAcl.getType() == this.getType();
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

  /**
   * ACL rights.
   */
  public enum OzoneACLRights {
    READ, WRITE, READ_WRITE;

    /**
     * Returns the ACL rights based on passed in String.
     *
     * @param type ACL right string
     *
     * @return OzoneACLRights
     */
    public static OzoneACLRights getACLRight(String type) {
      if (type == null || type.isEmpty()) {
        throw new IllegalArgumentException("ACL right cannot be empty");
      }

      switch (type) {
      case OzoneConsts.OZONE_ACL_READ:
        return OzoneACLRights.READ;
      case OzoneConsts.OZONE_ACL_WRITE:
        return OzoneACLRights.WRITE;
      case OzoneConsts.OZONE_ACL_READ_WRITE:
      case OzoneConsts.OZONE_ACL_WRITE_READ:
        return OzoneACLRights.READ_WRITE;
      default:
        throw new IllegalArgumentException("ACL right is not recognized");
      }

    }

    /**
     * Returns String representation of ACL rights.
     * @param acl OzoneACLRights
     * @return String representation of acl
     */
    public static String getACLRightsString(OzoneACLRights acl) {
      switch(acl) {
      case READ:
        return OzoneConsts.OZONE_ACL_READ;
      case WRITE:
        return OzoneConsts.OZONE_ACL_WRITE;
      case READ_WRITE:
        return OzoneConsts.OZONE_ACL_READ_WRITE;
      default:
        throw new IllegalArgumentException("ACL right is not recognized");
      }
    }

  }

}
