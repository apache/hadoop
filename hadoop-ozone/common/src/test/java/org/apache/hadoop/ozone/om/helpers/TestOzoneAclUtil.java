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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAclConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for OzoneAcls utility class.
 */
public class TestOzoneAclUtil {

  private static final List<OzoneAcl> DEFAULT_ACLS =
      getDefaultAcls(new OzoneConfiguration());

  private static final OzoneAcl USER1 = new OzoneAcl(USER, "user1",
      ACLType.READ_ACL, ACCESS);

  private static final OzoneAcl USER2 = new OzoneAcl(USER, "user2",
      ACLType.WRITE, ACCESS);

  private static final OzoneAcl GROUP1 = new OzoneAcl(GROUP, "group1",
      ACLType.ALL, ACCESS);

  @Test
  public void testAddAcl() throws IOException {
    List<OzoneAcl> currentAcls = getDefaultAcls(new OzoneConfiguration());
    assertTrue(currentAcls.size() > 0);

    // Add new permission to existing acl entry.
    OzoneAcl oldAcl = currentAcls.get(0);
    OzoneAcl newAcl = new OzoneAcl(oldAcl.getType(), oldAcl.getName(),
        ACLType.READ_ACL, ACCESS);

    addAndVerifyAcl(currentAcls, newAcl, true, DEFAULT_ACLS.size());
    // Add same permission again and verify result
    addAndVerifyAcl(currentAcls, newAcl, false, DEFAULT_ACLS.size());

    // Add a new user acl entry.
    addAndVerifyAcl(currentAcls, USER1, true, DEFAULT_ACLS.size() + 1);
    // Add same acl entry again and verify result
    addAndVerifyAcl(currentAcls, USER1, false, DEFAULT_ACLS.size() + 1);

    // Add a new group acl entry.
    addAndVerifyAcl(currentAcls, GROUP1, true, DEFAULT_ACLS.size() + 2);
    // Add same acl entry again and verify result
    addAndVerifyAcl(currentAcls, GROUP1, false, DEFAULT_ACLS.size() + 2);
  }

  @Test
  public void testRemoveAcl() {
    List<OzoneAcl> currentAcls = null;

    // add/remove to/from null OzoneAcls
    removeAndVerifyAcl(currentAcls, USER1, false, 0);
    addAndVerifyAcl(currentAcls, USER1, false, 0);
    removeAndVerifyAcl(currentAcls, USER1, false, 0);

    currentAcls = getDefaultAcls(new OzoneConfiguration());
    assertTrue(currentAcls.size() > 0);

    // Add new permission to existing acl entru.
    OzoneAcl oldAcl = currentAcls.get(0);
    OzoneAcl newAcl = new OzoneAcl(oldAcl.getType(), oldAcl.getName(),
        ACLType.READ_ACL, ACCESS);

    // Remove non existing acl entry
    removeAndVerifyAcl(currentAcls, USER1, false, DEFAULT_ACLS.size());

    // Remove non existing acl permission
    removeAndVerifyAcl(currentAcls, newAcl, false, DEFAULT_ACLS.size());

    // Add new permission to existing acl entry.
    addAndVerifyAcl(currentAcls, newAcl, true, DEFAULT_ACLS.size());

    // Remove the new permission added.
    removeAndVerifyAcl(currentAcls, newAcl, true, DEFAULT_ACLS.size());

    removeAndVerifyAcl(currentAcls, oldAcl, true, DEFAULT_ACLS.size() - 1);
  }

  private void addAndVerifyAcl(List<OzoneAcl> currentAcls, OzoneAcl addedAcl,
      boolean expectedResult, int expectedSize) {
    assertEquals(expectedResult, OzoneAclUtil.addAcl(currentAcls, addedAcl));
    if (currentAcls != null) {
      boolean verified = verifyAclAdded(currentAcls, addedAcl);
      assertTrue("addedAcl: " + addedAcl + " should exist in the" +
          " current acls: " + currentAcls, verified);
      assertEquals(expectedSize, currentAcls.size());
    }
  }

  private void removeAndVerifyAcl(List<OzoneAcl> currentAcls,
      OzoneAcl removedAcl, boolean expectedResult, int expectedSize) {
    assertEquals(expectedResult, OzoneAclUtil.removeAcl(currentAcls,
        removedAcl));
    if (currentAcls != null) {
      boolean verified = verifyAclRemoved(currentAcls, removedAcl);
      assertTrue("removedAcl: " + removedAcl + " should not exist in the" +
          " current acls: " + currentAcls, verified);
      assertEquals(expectedSize, currentAcls.size());
    }
  }

  private boolean verifyAclRemoved(List<OzoneAcl> acls, OzoneAcl removedAcl) {
    for (OzoneAcl acl : acls) {
      if (acl.getName().equals(removedAcl.getName()) &&
          acl.getType().equals(removedAcl.getType()) &&
          acl.getAclScope().equals(removedAcl.getAclScope())) {
        BitSet temp = (BitSet) acl.getAclBitSet().clone();
        temp.and(removedAcl.getAclBitSet());
        return !temp.equals(removedAcl.getAclBitSet());
      }
    }
    return true;
  }

  private boolean verifyAclAdded(List<OzoneAcl> acls, OzoneAcl newAcl) {
    for (OzoneAcl acl : acls) {
      if (acl.getName().equals(newAcl.getName()) &&
          acl.getType().equals(newAcl.getType()) &&
          acl.getAclScope().equals(newAcl.getAclScope())) {
        BitSet temp = (BitSet) acl.getAclBitSet().clone();
        temp.and(newAcl.getAclBitSet());
        return temp.equals(newAcl.getAclBitSet());
      }
    }
    return false;
  }

  /**
   * Helper function to get default acl list for current user.
   *
   * @return list of ozoneAcls.
   * @throws IOException
   * */
  private static List<OzoneAcl> getDefaultAcls(OzoneConfiguration conf) {
    List<OzoneAcl> ozoneAcls = new ArrayList<>();
    //User ACL
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      ugi = UserGroupInformation.createRemoteUser("user0");
    }

    OzoneAclConfig aclConfig = conf.getObject(OzoneAclConfig.class);
    IAccessAuthorizer.ACLType userRights = aclConfig.getUserDefaultRights();
    IAccessAuthorizer.ACLType groupRights = aclConfig.getGroupDefaultRights();

    OzoneAclUtil.addAcl(ozoneAcls, new OzoneAcl(USER,
        ugi.getUserName(), userRights, ACCESS));
    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(ugi.getGroupNames());
    userGroups.stream().forEach((group) -> OzoneAclUtil.addAcl(ozoneAcls,
        new OzoneAcl(GROUP, group, groupRights, ACCESS)));
    return ozoneAcls;
  }
}
