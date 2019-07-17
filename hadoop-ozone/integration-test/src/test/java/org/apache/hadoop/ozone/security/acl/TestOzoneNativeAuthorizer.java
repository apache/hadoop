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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.BucketManagerImpl;
import org.apache.hadoop.ozone.om.IOzoneAcl;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.VolumeManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.ANONYMOUS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.WORLD;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.PREFIX;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link OzoneNativeAuthorizer}.
 */
@RunWith(Parameterized.class)
public class TestOzoneNativeAuthorizer {

  private static OzoneConfiguration ozConfig;
  private String vol;
  private String buck;
  private String key;
  private String prefix;
  private ACLType parentDirUserAcl;
  private ACLType parentDirGroupAcl;
  private boolean expectedAclResult;

  private static KeyManagerImpl keyManager;
  private static VolumeManagerImpl volumeManager;
  private static BucketManagerImpl bucketManager;
  private static PrefixManager prefixManager;
  private static OMMetadataManager metadataManager;
  private static OzoneNativeAuthorizer nativeAuthorizer;

  private static StorageContainerManager scm;
  private static UserGroupInformation ugi;

  private static OzoneObj volObj;
  private static OzoneObj buckObj;
  private static OzoneObj keyObj;
  private static OzoneObj prefixObj;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"key", "dir1/", ALL, ALL, true},
        {"file1", "2019/june/01/", ALL, ALL, true},
        {"file2", "", ALL, ALL, true},
        {"dir1/dir2/dir4/", "", ALL, ALL, true},
        {"key", "dir1/", NONE, NONE, false},
        {"file1", "2019/june/01/", NONE, NONE, false},
        {"file2", "", NONE, NONE, false},
        {"dir1/dir2/dir4/", "", NONE, NONE, false}
    });
  }

  public TestOzoneNativeAuthorizer(String keyName, String prefixName,
      ACLType userRight,
      ACLType groupRight, boolean expectedResult) throws IOException {
    int randomInt = RandomUtils.nextInt();
    vol = "vol" + randomInt;
    buck = "bucket" + randomInt;
    key = keyName + randomInt;
    prefix = prefixName + randomInt + OZONE_URI_DELIMITER;
    parentDirUserAcl = userRight;
    parentDirGroupAcl = groupRight;
    expectedAclResult = expectedResult;

    createVolume(vol);
    createBucket(vol, buck);
    createKey(vol, buck, key);
  }

  @BeforeClass
  public static void setup() throws Exception {
    ozConfig = new OzoneConfiguration();
    ozConfig.set(OZONE_ACL_AUTHORIZER_CLASS,
        OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    File dir = GenericTestUtils.getRandomizedTestDir();
    ozConfig.set(OZONE_METADATA_DIRS, dir.toString());
    ozConfig.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);

    metadataManager = new OmMetadataManagerImpl(ozConfig);
    volumeManager = new VolumeManagerImpl(metadataManager, ozConfig);
    bucketManager = new BucketManagerImpl(metadataManager);
    prefixManager = new PrefixManagerImpl(metadataManager);

    NodeManager nodeManager = new MockNodeManager(true, 10);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setScmNodeManager(nodeManager);
    scm = TestUtils.getScm(ozConfig, configurator);
    scm.start();
    scm.exitSafeMode();
    keyManager =
        new KeyManagerImpl(scm.getBlockProtocolServer(), metadataManager,
            ozConfig,
            "om1", null);

    nativeAuthorizer = new OzoneNativeAuthorizer(volumeManager, bucketManager,
        keyManager, prefixManager);
    //keySession.
    ugi = UserGroupInformation.getCurrentUser();
  }

  private void createKey(String volume,
      String bucket, String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(keyName)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setDataSize(0)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setAcls(OzoneUtils.getAclList(ugi.getUserName(), ugi.getGroups(),
            ALL, ALL))
        .build();

    if (keyName.split(OZONE_URI_DELIMITER).length > 1) {
      keyManager.createDirectory(keyArgs);
      key = key + OZONE_URI_DELIMITER;
    } else {
      OpenKeySession keySession = keyManager.createFile(keyArgs, true, false);
      keyArgs.setLocationInfoList(
          keySession.getKeyInfo().getLatestVersionLocations()
              .getLocationList());
      keyManager.commitKey(keyArgs, keySession.getId());
    }

    keyObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setBucketName(buck)
        .setKeyName(key)
        .setResType(KEY)
        .setStoreType(OZONE)
        .build();
  }

  private void createBucket(String volumeName, String bucketName)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    bucketManager.createBucket(bucketInfo);
    buckObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setBucketName(buck)
        .setResType(BUCKET)
        .setStoreType(OZONE)
        .build();
  }

  private void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    volumeManager.createVolume(volumeArgs);
    volObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .build();
  }

  @Test
  public void testCheckAccessForVolume() throws Exception {
    expectedAclResult = true;
    resetAclsAndValidateAccess(volObj, USER, volumeManager);
    resetAclsAndValidateAccess(volObj, GROUP, volumeManager);
    resetAclsAndValidateAccess(volObj, WORLD, volumeManager);
    resetAclsAndValidateAccess(volObj, ANONYMOUS, volumeManager);
  }

  @Test
  public void testCheckAccessForBucket() throws Exception {

    OzoneAcl userAcl = new OzoneAcl(USER, ugi.getUserName(), parentDirUserAcl,
        ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(GROUP, ugi.getGroups().size() > 0 ?
        ugi.getGroups().get(0) : "", parentDirGroupAcl, ACCESS);
    // Set access for volume.
    volumeManager.setAcl(volObj, Arrays.asList(userAcl, groupAcl));

    resetAclsAndValidateAccess(buckObj, USER, bucketManager);
    resetAclsAndValidateAccess(buckObj, GROUP, bucketManager);
    resetAclsAndValidateAccess(buckObj, WORLD, bucketManager);
    resetAclsAndValidateAccess(buckObj, ANONYMOUS, bucketManager);
  }

  @Test
  public void testCheckAccessForKey() throws Exception {
    OzoneAcl userAcl = new OzoneAcl(USER, ugi.getUserName(), parentDirUserAcl,
        ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(GROUP, ugi.getGroups().size() > 0 ?
        ugi.getGroups().get(0) : "", parentDirGroupAcl, ACCESS);
    // Set access for volume, bucket & prefix.
    volumeManager.setAcl(volObj, Arrays.asList(userAcl, groupAcl));
    bucketManager.setAcl(buckObj, Arrays.asList(userAcl, groupAcl));
    //prefixManager.setAcl(prefixObj, Arrays.asList(userAcl, groupAcl));

    resetAclsAndValidateAccess(keyObj, USER, keyManager);
    resetAclsAndValidateAccess(keyObj, GROUP, keyManager);
    resetAclsAndValidateAccess(keyObj, WORLD, keyManager);
    resetAclsAndValidateAccess(keyObj, ANONYMOUS, keyManager);
  }

  @Test
  public void testCheckAccessForPrefix() throws Exception {
    prefixObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setBucketName(buck)
        .setPrefixName(prefix)
        .setResType(PREFIX)
        .setStoreType(OZONE)
        .build();

    OzoneAcl userAcl = new OzoneAcl(USER, ugi.getUserName(), parentDirUserAcl,
        ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(GROUP, ugi.getGroups().size() > 0 ?
        ugi.getGroups().get(0) : "", parentDirGroupAcl, ACCESS);
    // Set access for volume & bucket.
    volumeManager.setAcl(volObj, Arrays.asList(userAcl, groupAcl));
    bucketManager.setAcl(buckObj, Arrays.asList(userAcl, groupAcl));

    resetAclsAndValidateAccess(prefixObj, USER, prefixManager);
    resetAclsAndValidateAccess(prefixObj, GROUP, prefixManager);
    resetAclsAndValidateAccess(prefixObj, WORLD, prefixManager);
    resetAclsAndValidateAccess(prefixObj, ANONYMOUS, prefixManager);
  }

  private void resetAclsAndValidateAccess(OzoneObj obj,
      ACLIdentityType accessType, IOzoneAcl aclImplementor)
      throws IOException {

    List<OzoneAcl> acls;
    String user = "";
    String group = "";

    user = ugi.getUserName();
    if (ugi.getGroups().size() > 0) {
      group = ugi.getGroups().get(0);
    }

    RequestContext.Builder builder = new RequestContext.Builder()
        .setClientUgi(ugi)
        .setAclType(accessType);

    // Get all acls.
    List<ACLType> allAcls = Arrays.stream(ACLType.values()).
        collect(Collectors.toList());

    /**
     * 1. Reset default acls to an acl.
     * 2. Test if user/group has access only to it.
     * 3. Add remaining acls one by one and then test
     *    if user/group has access to them.
     * */
    for (ACLType a1 : allAcls) {
      OzoneAcl newAcl = new OzoneAcl(accessType, getAclName(accessType), a1,
          ACCESS);

      // Reset acls to only one right.
      aclImplementor.setAcl(obj, Arrays.asList(newAcl));

      // Fetch current acls and validate.
      acls = aclImplementor.getAcl(obj);
      assertTrue(acls.size() == 1);
      assertTrue(acls.contains(newAcl));

      // Special handling for ALL.
      if (a1.equals(ALL)) {
        validateAll(obj, builder);
        continue;
      }

      // Special handling for NONE.
      if (a1.equals(NONE)) {
        validateNone(obj, builder);
        continue;
      }
      assertEquals("Acl to check:" + a1 + " accessType:" +
              accessType + " path:" + obj.getPath(),
          expectedAclResult, nativeAuthorizer.checkAccess(obj,
              builder.setAclRights(a1).build()));

      List<ACLType> aclsToBeValidated =
          Arrays.stream(ACLType.values()).collect(Collectors.toList());
      List<ACLType> aclsToBeAdded =
          Arrays.stream(ACLType.values()).collect(Collectors.toList());
      aclsToBeValidated.remove(NONE);
      aclsToBeValidated.remove(a1);

      aclsToBeAdded.remove(NONE);
      aclsToBeAdded.remove(ALL);

      // Fetch acls again.
      for (ACLType a2 : aclsToBeAdded) {
        if (!a2.equals(a1)) {

          acls = aclImplementor.getAcl(obj);
          List right = acls.stream().map(a -> a.getAclList()).collect(
              Collectors.toList());
          assertFalse("Do not expected client to have " + a2 + " acl. " +
                  "Current acls found:" + right + ". Type:" + accessType + ","
                  + " name:" + (accessType == USER ? user : group),
              nativeAuthorizer.checkAccess(obj,
                  builder.setAclRights(a2).build()));

          // Randomize next type.
          int type = RandomUtils.nextInt(0, 3);
          ACLIdentityType identityType = ACLIdentityType.values()[type];
          // Add remaining acls one by one and then check access.
          OzoneAcl addAcl = new OzoneAcl(identityType, 
              getAclName(identityType), a2, ACCESS);
          aclImplementor.addAcl(obj, addAcl);

          // Fetch acls again.
          acls = aclImplementor.getAcl(obj);
          boolean a2AclFound = false;
          boolean a1AclFound = false;
          for (OzoneAcl acl : acls) {
            if (acl.getAclList().contains(a2)) {
              a2AclFound = true;
            }
            if (acl.getAclList().contains(a1)) {
              a1AclFound = true;
            }
          }

          assertTrue("Current acls :" + acls + ". " +
              "Type:" + accessType + ", name:" + (accessType == USER ? user
              : group) + " acl:" + a2, a2AclFound);
          assertTrue("Expected client to have " + a1 + " acl. Current acls " +
              "found:" + acls + ". Type:" + accessType +
              ", name:" + (accessType == USER ? user : group), a1AclFound);
          assertEquals("Current acls " + acls + ". Expect acl:" + a2 +
                  " to be set? " + expectedAclResult + " accessType:"
                  + accessType, expectedAclResult,
              nativeAuthorizer.checkAccess(obj,
                  builder.setAclRights(a2).build()));
          aclsToBeValidated.remove(a2);
          for (ACLType a3 : aclsToBeValidated) {
            if (!a3.equals(a1) && !a3.equals(a2)) {
              assertFalse("User shouldn't have right " + a3 + ". " +
                      "Current acl rights for user:" + a1 + "," + a2,
                  nativeAuthorizer.checkAccess(obj,
                      builder.setAclRights(a3).build()));
            }
          }
        }
      }
    }

  }

  private String getAclName(ACLIdentityType identityType) {
    switch (identityType) {
    case USER:
      return ugi.getUserName();
    case GROUP:
      if (ugi.getGroups().size() > 0) {
        return ugi.getGroups().get(0);
      }
    default:
      return "";
    }
  }

  /**
   * Helper function to test acl rights with user/group had ALL acl bit set.
   * @param obj
   * @param builder
   */
  private void validateAll(OzoneObj obj, RequestContext.Builder
      builder) throws OMException {
    List<ACLType> allAcls = new ArrayList<>(Arrays.asList(ACLType.values()));
    allAcls.remove(ALL);
    allAcls.remove(NONE);
    for (ACLType a : allAcls) {
      assertEquals("User should have right " + a + ".", 
          nativeAuthorizer.checkAccess(obj,
          builder.setAclRights(a).build()), expectedAclResult);
    }
  }

  /**
   * Helper function to test acl rights with user/group had NONE acl bit set.
   * @param obj
   * @param builder
   */
  private void validateNone(OzoneObj obj, RequestContext.Builder
      builder) throws OMException {
    List<ACLType> allAcls = new ArrayList<>(Arrays.asList(ACLType.values()));
    allAcls.remove(NONE);
    for (ACLType a : allAcls) {
      assertFalse("User shouldn't have right " + a + ".", 
          nativeAuthorizer.checkAccess(obj, builder.setAclRights(a).build()));
    }
  }
}