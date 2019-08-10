/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;

import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;

/**
 * Test class for @{@link KeyManagerImpl}.
 */
public class TestKeyManagerImpl {

  private static PrefixManager prefixManager;
  private static KeyManagerImpl keyManager;
  private static VolumeManagerImpl volumeManager;
  private static BucketManagerImpl bucketManager;
  private static NodeManager nodeManager;
  private static StorageContainerManager scm;
  private static ScmBlockLocationProtocol mockScmBlockLocationProtocol;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static File dir;
  private static long scmBlockSize;
  private static final String KEY_NAME = "key1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String VOLUME_NAME = "vol1";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    dir = GenericTestUtils.getRandomizedTestDir();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    mockScmBlockLocationProtocol = Mockito.mock(ScmBlockLocationProtocol.class);
    metadataManager = new OmMetadataManagerImpl(conf);
    volumeManager = new VolumeManagerImpl(metadataManager, conf);
    bucketManager = new BucketManagerImpl(metadataManager);
    nodeManager = new MockNodeManager(true, 10);
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager schemaManager = NodeSchemaManager.getInstance();
    schemaManager.init(schemas, false);
    NetworkTopology clusterMap = new NetworkTopologyImpl(schemaManager);
    nodeManager.getAllNodes().stream().forEach(node -> {
      node.setNetworkName(node.getUuidString());
      clusterMap.add(node);
    });
    ((MockNodeManager)nodeManager).setNetworkTopology(clusterMap);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setScmNodeManager(nodeManager);
    configurator.setNetworkTopology(clusterMap);
    scm = TestUtils.getScm(conf, configurator);
    scm.start();
    scm.exitSafeMode();
    scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
    conf.setLong(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, 10);

    keyManager =
        new KeyManagerImpl(scm.getBlockProtocolServer(), metadataManager, conf,
            "om1", null);
    prefixManager = new PrefixManagerImpl(metadataManager);

    Mockito.when(mockScmBlockLocationProtocol
        .allocateBlock(Mockito.anyLong(), Mockito.anyInt(),
            Mockito.any(ReplicationType.class),
            Mockito.any(ReplicationFactor.class), Mockito.anyString(),
            Mockito.any(ExcludeList.class))).thenThrow(
        new SCMException("SafeModePrecheck failed for allocateBlock",
            ResultCodes.SAFE_MODE_EXCEPTION));
    createVolume(VOLUME_NAME);
    createBucket(VOLUME_NAME, BUCKET_NAME);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    scm.stop();
    scm.join();
    metadataManager.stop();
    keyManager.stop();
    FileUtils.deleteDirectory(dir);
  }

  @After
  public void cleanupTest() throws IOException {
    List<OzoneFileStatus> fileStatuses = keyManager
        .listStatus(createBuilder().setKeyName("").build(), true, "", 100000);
    for (OzoneFileStatus fileStatus : fileStatuses) {
      if (fileStatus.isFile()) {
        keyManager.deleteKey(
            createKeyArgs(fileStatus.getPath().toString().substring(1)));
      } else {
        keyManager.deleteKey(createKeyArgs(OzoneFSUtils
            .addTrailingSlashIfNeeded(
                fileStatus.getPath().toString().substring(1))));
      }
    }
  }

  private static void createBucket(String volumeName, String bucketName)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    bucketManager.createBucket(bucketInfo);
  }

  private static void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    volumeManager.createVolume(volumeArgs);
  }

  @Test
  public void allocateBlockFailureInSafeMode() throws Exception {
    KeyManager keyManager1 = new KeyManagerImpl(mockScmBlockLocationProtocol,
        metadataManager, conf, "om1", null);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(KEY_NAME)
        .build();

    // As now openKey will allocate at least one block, even if the size
    // passed is 0. So adding an entry to openKeyTable manually to test
    // allocateBlock failure.
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationType(keyArgs.getType())
        .setReplicationFactor(keyArgs.getFactor())
        .setFileEncryptionInfo(null).build();
    metadataManager.getOpenKeyTable().put(
        metadataManager.getOpenKey(VOLUME_NAME, BUCKET_NAME, KEY_NAME, 1L),
        omKeyInfo);
    LambdaTestUtils.intercept(OMException.class,
        "SafeModePrecheck failed for allocateBlock", () -> {
          keyManager1
              .allocateBlock(keyArgs, 1L, new ExcludeList());
        });
  }

  @Test
  public void openKeyFailureInSafeMode() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    KeyManager keyManager1 = new KeyManagerImpl(mockScmBlockLocationProtocol,
        metadataManager, conf, "om1", null);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(KEY_NAME)
        .setDataSize(1000)
        .setAcls(OzoneUtils.getAclList(ugi.getUserName(), ugi.getGroups(),
            ALL, ALL))
        .build();
    LambdaTestUtils.intercept(OMException.class,
        "SafeModePrecheck failed for allocateBlock", () -> {
          keyManager1.openKey(keyArgs);
        });
  }

  @Test
  public void openKeyWithMultipleBlocks() throws IOException {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(UUID.randomUUID().toString())
        .setDataSize(scmBlockSize * 10)
        .build();
    OpenKeySession keySession = keyManager.openKey(keyArgs);
    OmKeyInfo keyInfo = keySession.getKeyInfo();
    Assert.assertEquals(10,
        keyInfo.getLatestVersionLocations().getLocationList().size());
  }

  @Test
  public void testCreateDirectory() throws IOException {
    // Create directory where the parent directory does not exist
    String keyName = RandomStringUtils.randomAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    for (int i =0; i< 5; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }
    keyManager.createDirectory(keyArgs);
    Path path = Paths.get(keyName);
    while (path != null) {
      // verify parent directories are created
      Assert.assertTrue(keyManager.getFileStatus(keyArgs).isDirectory());
      path = path.getParent();
    }

    // make sure create directory fails where parent is a file
    keyName = RandomStringUtils.randomAlphabetic(5);
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    OpenKeySession keySession = keyManager.openKey(keyArgs);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());
    for (int i =0; i< 5; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }
    try {
      keyManager.createDirectory(keyArgs);
      Assert.fail("Creation should fail for directory.");
    } catch (OMException e) {
      Assert.assertEquals(e.getResult(),
          OMException.ResultCodes.FILE_ALREADY_EXISTS);
    }

    // create directory for root directory
    keyName = "";
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    keyManager.createDirectory(keyArgs);
    Assert.assertTrue(keyManager.getFileStatus(keyArgs).isDirectory());

    // create directory where parent is root
    keyName = RandomStringUtils.randomAlphabetic(5);
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    keyManager.createDirectory(keyArgs);
    Assert.assertTrue(keyManager.getFileStatus(keyArgs).isDirectory());
  }

  @Test
  public void testOpenFile() throws IOException {
    // create key
    String keyName = RandomStringUtils.randomAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    OpenKeySession keySession = keyManager.createFile(keyArgs, false, false);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());

    // try to open created key with overWrite flag set to false
    try {
      keyManager.createFile(keyArgs, false, false);
      Assert.fail("Open key should fail for non overwrite create");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.FILE_ALREADY_EXISTS) {
        throw ex;
      }
    }

    // create file should pass with overwrite flag set to true
    keyManager.createFile(keyArgs, true, false);

    // try to create a file where parent directories do not exist and
    // recursive flag is set to false
    keyName = RandomStringUtils.randomAlphabetic(5);
    for (int i =0; i< 5; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    try {
      keyManager.createFile(keyArgs, false, false);
      Assert.fail("Open file should fail for non recursive write");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.DIRECTORY_NOT_FOUND) {
        throw ex;
      }
    }

    // file create should pass when recursive flag is set to true
    keySession = keyManager.createFile(keyArgs, false, true);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());
    Assert.assertTrue(keyManager
        .getFileStatus(keyArgs).isFile());

    // try creating a file over a directory
    keyArgs = createBuilder()
        .setKeyName("")
        .build();
    try {
      keyManager.createFile(keyArgs, true, true);
      Assert.fail("Open file should fail for non recursive write");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.NOT_A_FILE) {
        throw ex;
      }
    }
  }

  @Test
  public void testCheckAccessForFileKey() throws Exception {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("testdir/deep/NOTICE.txt")
        .build();
    OpenKeySession keySession = keyManager.createFile(keyArgs, false, true);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());

    OzoneObj fileKey = OzoneObjInfo.Builder.fromKeyArgs(keyArgs)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    RequestContext context = currentUserReads();
    Assert.assertTrue(keyManager.checkAccess(fileKey, context));

    OzoneObj parentDirKey = OzoneObjInfo.Builder.fromKeyArgs(keyArgs)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setKeyName("testdir")
        .build();
    Assert.assertTrue(keyManager.checkAccess(parentDirKey, context));
  }

  @Test
  public void testCheckAccessForNonExistentKey() throws Exception {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("testdir/deep/NO_SUCH_FILE.txt")
        .build();
    OzoneObj nonExistentKey = OzoneObjInfo.Builder.fromKeyArgs(keyArgs)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    OzoneTestUtils.expectOmException(OMException.ResultCodes.KEY_NOT_FOUND,
        () -> keyManager.checkAccess(nonExistentKey, currentUserReads()));
  }

  @Test
  public void testCheckAccessForDirectoryKey() throws Exception {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("some/dir")
        .build();
    keyManager.createDirectory(keyArgs);

    OzoneObj dirKey = OzoneObjInfo.Builder.fromKeyArgs(keyArgs)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    Assert.assertTrue(keyManager.checkAccess(dirKey, currentUserReads()));
  }

  @Test
  public void testPrefixAclOps() throws IOException {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix1 = "pf1/";

    OzoneObj ozPrefix1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneAcl ozAcl1 = new OzoneAcl(ACLIdentityType.USER, "user1",
        ACLType.READ, ACCESS);
    prefixManager.addAcl(ozPrefix1, ozAcl1);

    List<OzoneAcl> ozAclGet = prefixManager.getAcl(ozPrefix1);
    Assert.assertEquals(1, ozAclGet.size());
    Assert.assertEquals(ozAcl1, ozAclGet.get(0));

    List<OzoneAcl> acls = new ArrayList<>();
    OzoneAcl ozAcl2 = new OzoneAcl(ACLIdentityType.USER, "admin",
        ACLType.ALL, ACCESS);

    BitSet rwRights = new BitSet();
    rwRights.set(IAccessAuthorizer.ACLType.WRITE.ordinal());
    rwRights.set(IAccessAuthorizer.ACLType.READ.ordinal());
    OzoneAcl ozAcl3 = new OzoneAcl(ACLIdentityType.GROUP, "dev",
        rwRights, ACCESS);

    BitSet wRights = new BitSet();
    wRights.set(IAccessAuthorizer.ACLType.WRITE.ordinal());
    OzoneAcl ozAcl4 = new OzoneAcl(ACLIdentityType.GROUP, "dev",
        wRights, ACCESS);

    BitSet rRights = new BitSet();
    rRights.set(IAccessAuthorizer.ACLType.READ.ordinal());
    OzoneAcl ozAcl5 = new OzoneAcl(ACLIdentityType.GROUP, "dev",
        rRights, ACCESS);

    acls.add(ozAcl2);
    acls.add(ozAcl3);

    prefixManager.setAcl(ozPrefix1, acls);
    ozAclGet = prefixManager.getAcl(ozPrefix1);
    Assert.assertEquals(2, ozAclGet.size());

    int matchEntries = 0;
    for (OzoneAcl acl : ozAclGet) {
      if (acl.getType() == ACLIdentityType.GROUP) {
        Assert.assertEquals(ozAcl3, acl);
        matchEntries++;
      }
      if (acl.getType() == ACLIdentityType.USER) {
        Assert.assertEquals(ozAcl2, acl);
        matchEntries++;
      }
    }
    Assert.assertEquals(2, matchEntries);

    boolean result = prefixManager.removeAcl(ozPrefix1, ozAcl4);
    Assert.assertEquals(true, result);

    ozAclGet = prefixManager.getAcl(ozPrefix1);
    Assert.assertEquals(2, ozAclGet.size());

    result = prefixManager.removeAcl(ozPrefix1, ozAcl3);
    Assert.assertEquals(true, result);
    ozAclGet = prefixManager.getAcl(ozPrefix1);
    Assert.assertEquals(1, ozAclGet.size());

    Assert.assertEquals(ozAcl2, ozAclGet.get(0));

    // add dev:w
    prefixManager.addAcl(ozPrefix1, ozAcl4);
    ozAclGet = prefixManager.getAcl(ozPrefix1);
    Assert.assertEquals(2, ozAclGet.size());

    // add dev:r and validate the acl bitset combined
    prefixManager.addAcl(ozPrefix1, ozAcl5);
    ozAclGet = prefixManager.getAcl(ozPrefix1);
    Assert.assertEquals(2, ozAclGet.size());

    matchEntries = 0;
    for (OzoneAcl acl : ozAclGet) {
      if (acl.getType() == ACLIdentityType.GROUP) {
        Assert.assertEquals(ozAcl3, acl);
        matchEntries++;
      }
      if (acl.getType() == ACLIdentityType.USER) {
        Assert.assertEquals(ozAcl2, acl);
        matchEntries++;
      }
    }
    Assert.assertEquals(2, matchEntries);
  }

  @Test
  public void testInvalidPrefixAcl() throws IOException {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix1 = "pf1/";

    // Invalid prefix not ending with "/"
    String invalidPrefix = "invalid/pf";
    OzoneAcl ozAcl1 = new OzoneAcl(ACLIdentityType.USER, "user1",
        ACLType.READ, ACCESS);

    OzoneObj ozInvalidPrefix = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(invalidPrefix)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // add acl with invalid prefix name
    exception.expect(OMException.class);
    exception.expectMessage("Invalid prefix name");
    prefixManager.addAcl(ozInvalidPrefix, ozAcl1);

    OzoneObj ozPrefix1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();


    List<OzoneAcl> ozAclGet = prefixManager.getAcl(ozPrefix1);
    Assert.assertEquals(1, ozAclGet.size());
    Assert.assertEquals(ozAcl1, ozAclGet.get(0));

    // get acl with invalid prefix name
    exception.expect(OMException.class);
    exception.expectMessage("Invalid prefix name");
    ozAclGet = prefixManager.getAcl(ozInvalidPrefix);
    Assert.assertEquals(null, ozAcl1);

    // set acl with invalid prefix name
    List<OzoneAcl> ozoneAcls = new ArrayList<OzoneAcl>();
    ozoneAcls.add(ozAcl1);
    exception.expect(OMException.class);
    exception.expectMessage("Invalid prefix name");
    prefixManager.setAcl(ozInvalidPrefix, ozoneAcls);

    // remove acl with invalid prefix name
    exception.expect(OMException.class);
    exception.expectMessage("Invalid prefix name");
    prefixManager.removeAcl(ozInvalidPrefix, ozAcl1);
  }

  @Test
  public void testLongestPrefixPath() throws IOException {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix1 = "pf1/pf11/pf111/pf1111/";
    String file1 = "pf1/pf11/file1";
    String file2 = "pf1/pf11/pf111/pf1111/file2";

    OzoneObj ozPrefix1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneAcl ozAcl1 = new OzoneAcl(ACLIdentityType.USER, "user1",
        ACLType.READ, ACCESS);
    prefixManager.addAcl(ozPrefix1, ozAcl1);

    OzoneObj ozFile1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(file1)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    List<OmPrefixInfo> prefixInfos =
        prefixManager.getLongestPrefixPath(ozFile1.getPath());
    Assert.assertEquals(5, prefixInfos.size());

    OzoneObj ozFile2 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(file2)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    prefixInfos =
        prefixManager.getLongestPrefixPath(ozFile2.getPath());
    Assert.assertEquals(7, prefixInfos.size());
    // Only the last node has acl on it
    Assert.assertEquals(ozAcl1, prefixInfos.get(6).getAcls().get(0));
    // All other nodes don't have acl value associate with it
    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(null, prefixInfos.get(i));
    }
  }

  @Test
  public void testLookupFile() throws IOException {
    String keyName = RandomStringUtils.randomAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();

    // lookup for a non-existent file
    try {
      keyManager.lookupFile(keyArgs, null);
      Assert.fail("Lookup file should fail for non existent file");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.FILE_NOT_FOUND) {
        throw ex;
      }
    }

    // create a file
    OpenKeySession keySession = keyManager.createFile(keyArgs, false, false);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());
    Assert.assertEquals(keyManager.lookupFile(keyArgs, null).getKeyName(),
        keyName);

    // lookup for created file
    keyArgs = createBuilder()
        .setKeyName("")
        .build();
    try {
      keyManager.lookupFile(keyArgs, null);
      Assert.fail("Lookup file should fail for a directory");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.NOT_A_FILE) {
        throw ex;
      }
    }
  }

  private OmKeyArgs createKeyArgs(String toKeyName) throws IOException {
    return createBuilder().setKeyName(toKeyName).build();
  }

  @Test
  public void testLookupKeyWithLocation() throws IOException {
    String keyName = RandomStringUtils.randomAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(true)
        .build();

    // lookup for a non-existent key
    try {
      keyManager.lookupKey(keyArgs, null);
      Assert.fail("Lookup key should fail for non existent key");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.KEY_NOT_FOUND) {
        throw ex;
      }
    }

    // create a key
    OpenKeySession keySession = keyManager.createFile(keyArgs, false, false);
    // randomly select 3 datanodes
    List<DatanodeDetails> nodeList = new ArrayList<>();
    nodeList.add((DatanodeDetails)scm.getClusterMap().getNode(
        0, null, null, null, null, 0));
    nodeList.add((DatanodeDetails)scm.getClusterMap().getNode(
        1, null, null, null, null, 0));
    nodeList.add((DatanodeDetails)scm.getClusterMap().getNode(
        2, null, null, null, null, 0));
    Assume.assumeFalse(nodeList.get(0).equals(nodeList.get(1)));
    Assume.assumeFalse(nodeList.get(0).equals(nodeList.get(2)));
    // create a pipeline using 3 datanodes
    Pipeline pipeline = scm.getPipelineManager().createPipeline(
        ReplicationType.RATIS, ReplicationFactor.THREE, nodeList);
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    locationInfoList.add(
        new OmKeyLocationInfo.Builder().setPipeline(pipeline)
            .setBlockID(new BlockID(1L, 1L)).build());
    keyArgs.setLocationInfoList(locationInfoList);

    keyManager.commitKey(keyArgs, keySession.getId());

    OmKeyInfo key = keyManager.lookupKey(keyArgs, null);
    Assert.assertEquals(key.getKeyName(), keyName);
    List<OmKeyLocationInfo> keyLocations =
        key.getLatestVersionLocations().getLocationList();
    DatanodeDetails leader =
        keyLocations.get(0).getPipeline().getFirstNode();
    DatanodeDetails follower1 =
        keyLocations.get(0).getPipeline().getNodes().get(1);
    DatanodeDetails follower2 =
        keyLocations.get(0).getPipeline().getNodes().get(2);
    Assert.assertNotEquals(leader, follower1);
    Assert.assertNotEquals(follower1, follower2);

    // lookup key, leader as client
    OmKeyInfo key1 = keyManager.lookupKey(keyArgs, leader.getIpAddress());
    Assert.assertEquals(leader, key1.getLatestVersionLocations()
        .getLocationList().get(0).getPipeline().getClosestNode());

    // lookup key, follower1 as client
    OmKeyInfo key2 = keyManager.lookupKey(keyArgs, follower1.getIpAddress());
    Assert.assertEquals(follower1, key2.getLatestVersionLocations()
        .getLocationList().get(0).getPipeline().getClosestNode());

    // lookup key, follower2 as client
    OmKeyInfo key3 = keyManager.lookupKey(keyArgs, follower2.getIpAddress());
    Assert.assertEquals(follower2, key3.getLatestVersionLocations()
        .getLocationList().get(0).getPipeline().getClosestNode());

    // lookup key, random node as client
    OmKeyInfo key4 = keyManager.lookupKey(keyArgs,
        "/d=default-drack/127.0.0.1");
    Assert.assertEquals(leader, key4.getLatestVersionLocations()
        .getLocationList().get(0).getPipeline().getClosestNode());
  }

  @Test
  public void testListStatus() throws IOException {
    String superDir = RandomStringUtils.randomAlphabetic(5);

    int numDirectories = 5;
    int numFiles = 5;
    // set of directory descendants of root
    Set<String> directorySet = new TreeSet<>();
    // set of file descendants of root
    Set<String> fileSet = new TreeSet<>();
    createDepthTwoDirectory(superDir, numDirectories, numFiles, directorySet,
        fileSet);
    // set of all descendants of root
    Set<String> children = new TreeSet<>(directorySet);
    children.addAll(fileSet);
    // number of entries in the filesystem
    int numEntries = directorySet.size() + fileSet.size();

    OmKeyArgs rootDirArgs = createKeyArgs("");
    List<OzoneFileStatus> fileStatuses =
        keyManager.listStatus(rootDirArgs, true, "", 100);
    // verify the number of status returned is same as number of entries
    Assert.assertEquals(numEntries, fileStatuses.size());

    fileStatuses = keyManager.listStatus(rootDirArgs, false, "", 100);
    // the number of immediate children of root is 1
    Assert.assertEquals(1, fileStatuses.size());

    // if startKey is the first descendant of the root then listStatus should
    // return all the entries.
    String startKey = children.iterator().next();
    fileStatuses = keyManager.listStatus(rootDirArgs, true,
        startKey.substring(0, startKey.length() - 1), 100);
    Assert.assertEquals(numEntries, fileStatuses.size());

    for (String directory : directorySet) {
      // verify status list received for each directory with recursive flag set
      // to false
      OmKeyArgs dirArgs = createKeyArgs(directory);
      fileStatuses = keyManager.listStatus(dirArgs, false, "", 100);
      verifyFileStatus(directory, fileStatuses, directorySet, fileSet, false);

      // verify status list received for each directory with recursive flag set
      // to true
      fileStatuses = keyManager.listStatus(dirArgs, true, "", 100);
      verifyFileStatus(directory, fileStatuses, directorySet, fileSet, true);

      // verify list status call with using the startKey parameter and
      // recursive flag set to false. After every call to listStatus use the
      // latest received file status as the startKey until no more entries are
      // left to list.
      List<OzoneFileStatus> tempFileStatus = null;
      Set<OzoneFileStatus> tmpStatusSet = new HashSet<>();
      do {
        tempFileStatus = keyManager.listStatus(dirArgs, false,
            tempFileStatus != null ? OzoneFSUtils.pathToKey(
                tempFileStatus.get(tempFileStatus.size() - 1).getPath()) : null,
            2);
        tmpStatusSet.addAll(tempFileStatus);
      } while (tempFileStatus.size() == 2);
      verifyFileStatus(directory, new ArrayList<>(tmpStatusSet), directorySet,
          fileSet, false);

      // verify list status call with using the startKey parameter and
      // recursive flag set to true. After every call to listStatus use the
      // latest received file status as the startKey until no more entries are
      // left to list.
      tempFileStatus = null;
      tmpStatusSet = new HashSet<>();
      do {
        tempFileStatus = keyManager.listStatus(dirArgs, true,
            tempFileStatus != null ? OzoneFSUtils.pathToKey(
                tempFileStatus.get(tempFileStatus.size() - 1).getPath()) : null,
            2);
        tmpStatusSet.addAll(tempFileStatus);
      } while (tempFileStatus.size() == 2);
      verifyFileStatus(directory, new ArrayList<>(tmpStatusSet), directorySet,
          fileSet, true);
    }
  }

  /**
   * Creates a depth two directory.
   *
   * @param superDir       Super directory to create
   * @param numDirectories number of directory children
   * @param numFiles       number of file children
   * @param directorySet   set of descendant directories for the super directory
   * @param fileSet        set of descendant files for the super directory
   */
  private void createDepthTwoDirectory(String superDir, int numDirectories,
      int numFiles, Set<String> directorySet, Set<String> fileSet)
      throws IOException {
    // create super directory
    OmKeyArgs superDirArgs = createKeyArgs(superDir);
    keyManager.createDirectory(superDirArgs);
    directorySet.add(superDir);

    // add directory children to super directory
    Set<String> childDirectories =
        createDirectories(superDir, new HashMap<>(), numDirectories);
    directorySet.addAll(childDirectories);
    // add file to super directory
    fileSet.addAll(createFiles(superDir, new HashMap<>(), numFiles));

    // for each child directory create files and directories
    for (String child : childDirectories) {
      fileSet.addAll(createFiles(child, new HashMap<>(), numFiles));
      directorySet
          .addAll(createDirectories(child, new HashMap<>(), numDirectories));
    }
  }

  private void verifyFileStatus(String directory,
      List<OzoneFileStatus> fileStatuses, Set<String> directorySet,
      Set<String> fileSet, boolean recursive) {

    for (OzoneFileStatus fileStatus : fileStatuses) {
      String keyName = OzoneFSUtils.pathToKey(fileStatus.getPath());
      String parent = Paths.get(keyName).getParent().toString();
      if (!recursive) {
        // if recursive is false, verify all the statuses have the input
        // directory as parent
        Assert.assertEquals(parent, directory);
      }
      // verify filestatus is present in directory or file set accordingly
      if (fileStatus.isDirectory()) {
        Assert.assertTrue(directorySet.contains(keyName));
      } else {
        Assert.assertTrue(fileSet.contains(keyName));
      }
    }

    // count the number of entries which should be present in the directory
    int numEntries = 0;
    Set<String> entrySet = new TreeSet<>(directorySet);
    entrySet.addAll(fileSet);
    for (String entry : entrySet) {
      if (OzoneFSUtils.getParent(entry)
          .startsWith(OzoneFSUtils.addTrailingSlashIfNeeded(directory))) {
        if (recursive) {
          numEntries++;
        } else if (OzoneFSUtils.getParent(entry)
            .equals(OzoneFSUtils.addTrailingSlashIfNeeded(directory))) {
          numEntries++;
        }
      }
    }
    // verify the number of entries match the status list size
    Assert.assertEquals(fileStatuses.size(), numEntries);
  }

  private Set<String> createDirectories(String parent,
      Map<String, List<String>> directoryMap, int numDirectories)
      throws IOException {
    Set<String> keyNames = new TreeSet<>();
    for (int i = 0; i < numDirectories; i++) {
      String keyName = parent + "/" + RandomStringUtils.randomAlphabetic(5);
      OmKeyArgs keyArgs = createBuilder().setKeyName(keyName).build();
      keyManager.createDirectory(keyArgs);
      keyNames.add(keyName);
    }
    directoryMap.put(parent, new ArrayList<>(keyNames));
    return keyNames;
  }

  private List<String> createFiles(String parent,
      Map<String, List<String>> fileMap, int numFiles) throws IOException {
    List<String> keyNames = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String keyName = parent + "/" + RandomStringUtils.randomAlphabetic(5);
      OmKeyArgs keyArgs = createBuilder().setKeyName(keyName).build();
      OpenKeySession keySession = keyManager.createFile(keyArgs, false, false);
      keyArgs.setLocationInfoList(
          keySession.getKeyInfo().getLatestVersionLocations()
              .getLocationList());
      keyManager.commitKey(keyArgs, keySession.getId());
      keyNames.add(keyName);
    }
    fileMap.put(parent, keyNames);
    return keyNames;
  }

  private OmKeyArgs.Builder createBuilder() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return new OmKeyArgs.Builder()
        .setBucketName(BUCKET_NAME)
        .setFactor(ReplicationFactor.ONE)
        .setDataSize(0)
        .setType(ReplicationType.STAND_ALONE)
        .setAcls(OzoneUtils.getAclList(ugi.getUserName(), ugi.getGroups(),
            ALL, ALL))
        .setVolumeName(VOLUME_NAME);
  }

  private RequestContext currentUserReads() throws IOException {
    return RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.getCurrentUser())
        .setAclRights(ACLType.READ_ACL)
        .setAclType(ACLIdentityType.USER)
        .build();
  }
}