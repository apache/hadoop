/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.RequestContext;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;

/**
 * Helper class for ozone acls operations.
 */
public final class OzoneAclUtil {

  private OzoneAclUtil(){
  }

  /**
   * Helper function to get access acl list for current user.
   *
   * @param userName
   * @param userGroups
   * @return list of OzoneAcls
   * */
  public static List<OzoneAcl> getAclList(String userName,
      List<String> userGroups, ACLType userRights, ACLType groupRights) {

    List<OzoneAcl> listOfAcls = new ArrayList<>();

    // User ACL.
    listOfAcls.add(new OzoneAcl(USER, userName, userRights, ACCESS));
    if(userGroups != null) {
      // Group ACLs of the User.
      userGroups.forEach((group) -> listOfAcls.add(
          new OzoneAcl(GROUP, group, groupRights, ACCESS)));
    }
    return listOfAcls;
  }

  /**
   * Check if acl right requested for given RequestContext exist
   * in provided acl list.
   * Acl validation rules:
   * 1. If user/group has ALL bit set than all user should have all rights.
   * 2. If user/group has NONE bit set than user/group will not have any right.
   * 3. For all other individual rights individual bits should be set.
   *
   * @param acls
   * @param context
   * @return return true if acl list contains right requsted in context.
   * */
  public static boolean checkAclRight(List<OzoneAcl> acls,
      RequestContext context) throws OMException {
    String[] userGroups = context.getClientUgi().getGroupNames();
    String userName = context.getClientUgi().getUserName();
    ACLType aclToCheck = context.getAclRights();
    for (OzoneAcl a : acls) {
      if(checkAccessInAcl(a, userGroups, userName, aclToCheck)) {
        return true;
      }
    }
    return false;
  }

  private static boolean checkAccessInAcl(OzoneAcl a, String[] groups,
      String username, ACLType aclToCheck) {
    BitSet rights = a.getAclBitSet();
    switch (a.getType()) {
    case USER:
      if (a.getName().equals(username)) {
        return checkIfAclBitIsSet(aclToCheck, rights);
      }
      break;
    case GROUP:
      for (String grp : groups) {
        if (a.getName().equals(grp)) {
          return checkIfAclBitIsSet(aclToCheck, rights);
        }
      }
      break;

    default:
      return checkIfAclBitIsSet(aclToCheck, rights);
    }
    return false;
  }

  /**
   * Check if acl right requested for given RequestContext exist
   * in provided acl list.
   * Acl validation rules:
   * 1. If user/group has ALL bit set than all user should have all rights.
   * 2. If user/group has NONE bit set than user/group will not have any right.
   * 3. For all other individual rights individual bits should be set.
   *
   * @param acls
   * @param context
   * @return return true if acl list contains right requsted in context.
   * */
  public static boolean checkAclRights(List<OzoneAcl> acls,
      RequestContext context) throws OMException {
    String[] userGroups = context.getClientUgi().getGroupNames();
    String userName = context.getClientUgi().getUserName();
    ACLType aclToCheck = context.getAclRights();
    for (OzoneAcl acl : acls) {
      if (checkAccessInAcl(acl, userGroups, userName, aclToCheck)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Helper function to check if bit for given acl is set.
   * @param acl
   * @param bitset
   * @return True of acl bit is set else false.
   * */
  public static boolean checkIfAclBitIsSet(IAccessAuthorizer.ACLType acl,
      BitSet bitset) {
    if (bitset == null) {
      return false;
    }

    return ((bitset.get(acl.ordinal())
        || bitset.get(ALL.ordinal()))
        && !bitset.get(NONE.ordinal()));
  }

  /**
   * Helper function to inherit default ACL as access ACL for child object.
   * 1. deep copy of OzoneAcl to avoid unexpected parent default ACL change
   * 2. merge inherited access ACL with existing access ACL via
   * OzoneUtils.addAcl().
   * @param acls
   * @param parentAcls
   * @return true if acls inherited DEFAULT acls from parentAcls successfully,
   * false otherwise.
   */
  public static boolean inheritDefaultAcls(List<OzoneAcl> acls,
      List<OzoneAcl> parentAcls) {
    List<OzoneAcl> inheritedAcls = null;
    if (parentAcls != null && !parentAcls.isEmpty()) {
      inheritedAcls = parentAcls.stream()
          .filter(a -> a.getAclScope() == DEFAULT)
          .map(acl -> new OzoneAcl(acl.getType(), acl.getName(),
              acl.getAclBitSet(), OzoneAcl.AclScope.ACCESS))
          .collect(Collectors.toList());
    }
    if (inheritedAcls != null && !inheritedAcls.isEmpty()) {
      inheritedAcls.stream().forEach(acl -> addAcl(acls, acl));
      return true;
    }
    return false;
  }

  /**
   * Convert a list of OzoneAclInfo(protoc) to list of OzoneAcl(java).
   * @param protoAcls
   * @return list of OzoneAcl.
   */
  public static List<OzoneAcl> fromProtobuf(List<OzoneAclInfo> protoAcls) {
    return protoAcls.stream().map(acl->OzoneAcl.fromProtobuf(acl))
        .collect(Collectors.toList());
  }

  /**
   * Convert a list of OzoneAcl(java) to list of OzoneAclInfo(protoc).
   * @param protoAcls
   * @return list of OzoneAclInfo.
   */
  public static List<OzoneAclInfo> toProtobuf(List<OzoneAcl> protoAcls) {
    return protoAcls.stream().map(acl->OzoneAcl.toProtobuf(acl))
        .collect(Collectors.toList());
  }

  /**
   * Add an OzoneAcl to existing list of OzoneAcls.
   * @param existingAcls
   * @param acl
   * @return true if current OzoneAcls are changed, false otherwise.
   */
  public static boolean addAcl(List<OzoneAcl> existingAcls, OzoneAcl acl) {
    if (existingAcls == null || acl == null) {
      return false;
    }

    for (OzoneAcl a: existingAcls) {
      if (a.getName().equals(acl.getName()) &&
          a.getType().equals(acl.getType()) &&
          a.getAclScope().equals(acl.getAclScope())) {
        BitSet current = a.getAclBitSet();
        BitSet original = (BitSet) current.clone();
        current.or(acl.getAclBitSet());
        if (current.equals(original)) {
          return false;
        }
        return true;
      }
    }

    existingAcls.add(acl);
    return true;
  }

  /**
   * remove OzoneAcl from existing list of OzoneAcls.
   * @param existingAcls
   * @param acl
   * @return true if current OzoneAcls are changed, false otherwise.
   */
  public static boolean removeAcl(List<OzoneAcl> existingAcls, OzoneAcl acl) {
    if (existingAcls == null || existingAcls.isEmpty() || acl == null) {
      return false;
    }

    for (OzoneAcl a: existingAcls) {
      if (a.getName().equals(acl.getName()) &&
          a.getType().equals(acl.getType()) &&
          a.getAclScope().equals(acl.getAclScope())) {
        BitSet current = a.getAclBitSet();
        BitSet original = (BitSet) current.clone();
        current.andNot(acl.getAclBitSet());

        if (current.equals(original)) {
          return false;
        }

        if (current.isEmpty()) {
          existingAcls.remove(a);
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Set existingAcls to newAcls.
   * @param existingAcls
   * @param newAcls
   * @return true if newAcls are set successfully, false otherwise.
   */
  public static boolean setAcl(List<OzoneAcl> existingAcls,
      List<OzoneAcl> newAcls) {
    if (existingAcls == null) {
      return false;
    } else {
      existingAcls.clear();
      if (newAcls != null) {
        existingAcls.addAll(newAcls);
      }
    }
    return true;
  }
}
