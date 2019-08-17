/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.util.BitSet;

/**
 * Public API for Ozone ACLs. Security providers providing support for Ozone
 * ACLs should implement this.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public interface IAccessAuthorizer {

  /**
   * Check access for given ozoneObject.
   *
   * @param ozoneObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @throws org.apache.hadoop.ozone.om.exceptions.OMException
   * @return true if user has access else false.
   */
  boolean checkAccess(IOzoneObj ozoneObject, RequestContext context)
      throws OMException;

  /**
   * ACL rights.
   */
  enum ACLType {
    READ,
    WRITE,
    CREATE,
    LIST,
    DELETE,
    READ_ACL,
    WRITE_ACL,
    ALL,
    NONE;
    private static int length = ACLType.values().length;
    private static ACLType[] vals = ACLType.values();

    public static int getNoOfAcls() {
      return length;
    }

    public static ACLType getAclTypeFromOrdinal(int ordinal) {
      if (ordinal > length - 1 && ordinal > -1) {
        throw new IllegalArgumentException("Ordinal greater than array lentgh" +
            ". ordinal:" + ordinal);
      }
      return vals[ordinal];
    }

    /**
     * Returns the ACL rights based on passed in String.
     *
     * @param type ACL right string
     * @return ACLType
     */
    public static ACLType getACLRight(String type) {
      if (type == null || type.isEmpty()) {
        throw new IllegalArgumentException("ACL right cannot be empty");
      }

      switch (type) {
      case OzoneConsts.OZONE_ACL_READ:
        return ACLType.READ;
      case OzoneConsts.OZONE_ACL_WRITE:
        return ACLType.WRITE;
      case OzoneConsts.OZONE_ACL_CREATE:
        return ACLType.CREATE;
      case OzoneConsts.OZONE_ACL_DELETE:
        return ACLType.DELETE;
      case OzoneConsts.OZONE_ACL_LIST:
        return ACLType.LIST;
      case OzoneConsts.OZONE_ACL_READ_ACL:
        return ACLType.READ_ACL;
      case OzoneConsts.OZONE_ACL_WRITE_ACL:
        return ACLType.WRITE_ACL;
      case OzoneConsts.OZONE_ACL_ALL:
        return ACLType.ALL;
      case OzoneConsts.OZONE_ACL_NONE:
        return ACLType.NONE;
      default:
        throw new IllegalArgumentException("[" + type + "] ACL right is not " +
            "recognized");
      }

    }

    /**
     * Returns String representation of ACL rights.
     *
     * @param acls ACLType
     * @return String representation of acl
     */
    public static String getACLString(BitSet acls) {
      StringBuffer sb = new StringBuffer();
      acls.stream().forEach(acl -> {
        sb.append(getAclString(ACLType.values()[acl]));
      });
      return sb.toString();
    }

    public static String getAclString(ACLType acl) {
      switch (acl) {
      case READ:
        return OzoneConsts.OZONE_ACL_READ;
      case WRITE:
        return OzoneConsts.OZONE_ACL_WRITE;
      case CREATE:
        return OzoneConsts.OZONE_ACL_CREATE;
      case DELETE:
        return OzoneConsts.OZONE_ACL_DELETE;
      case LIST:
        return OzoneConsts.OZONE_ACL_LIST;
      case READ_ACL:
        return OzoneConsts.OZONE_ACL_READ_ACL;
      case WRITE_ACL:
        return OzoneConsts.OZONE_ACL_WRITE_ACL;
      case ALL:
        return OzoneConsts.OZONE_ACL_ALL;
      case NONE:
        return OzoneConsts.OZONE_ACL_NONE;
      default:
        throw new IllegalArgumentException("ACL right is not recognized");
      }
    }

  }

  /**
   * Type of acl identity.
   */
  enum ACLIdentityType {
    USER(OzoneConsts.OZONE_ACL_USER_TYPE),
    GROUP(OzoneConsts.OZONE_ACL_GROUP_TYPE),
    WORLD(OzoneConsts.OZONE_ACL_WORLD_TYPE),
    ANONYMOUS(OzoneConsts.OZONE_ACL_ANONYMOUS_TYPE),
    CLIENT_IP(OzoneConsts.OZONE_ACL_IP_TYPE);

    // TODO: Add support for acl checks based on CLIENT_IP.

    @Override
    public String toString() {
      return value;
    }
    /**
     * String value for this Enum.
     */
    private final String value;

    /**
     * Init OzoneACLtypes enum.
     *
     * @param val String type for this enum.
     */
    ACLIdentityType(String val) {
      value = val;
    }
  }

}
