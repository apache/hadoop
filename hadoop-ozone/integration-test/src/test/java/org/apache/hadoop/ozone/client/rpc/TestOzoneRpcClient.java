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

package org.apache.hadoop.ozone.client.rpc;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.*;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueBlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocolPB.
    StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
public class TestOzoneRpcClient {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  /**
   * Create a MiniOzoneCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_CONTAINER_PROVISION_BATCH_SIZE, 1);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(10).build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
  }

  @Test
  public void testCreateVolume()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateVolumeWithOwner()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs.Builder argsBuilder = VolumeArgs.newBuilder();
    argsBuilder.setOwner("test");
    store.createVolume(volumeName, argsBuilder.build());
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(volumeName, volume.getName());
    Assert.assertEquals("test", volume.getOwner());
  }

  @Test
  public void testCreateVolumeWithQuota()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs.Builder argsBuilder = VolumeArgs.newBuilder();
    argsBuilder.setOwner("test").setQuota("1000000000 BYTES");
    store.createVolume(volumeName, argsBuilder.build());
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(volumeName, volume.getName());
    Assert.assertEquals("test", volume.getOwner());
    Assert.assertEquals(1000000000L, volume.getQuota());
  }

  @Test
  public void testInvalidVolumeCreation() throws IOException {
    thrown.expectMessage("Bucket or Volume name has an unsupported" +
        " character : #");
    String volumeName = "invalid#name";
    store.createVolume(volumeName);
  }

  @Test
  public void testVolumeAlreadyExist()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    try {
      store.createVolume(volumeName);
    } catch (IOException ex) {
      Assert.assertEquals(
          "Volume creation failed, error:VOLUME_ALREADY_EXISTS",
          ex.getMessage());
    }
  }

  @Test
  public void testSetVolumeOwner()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    store.getVolume(volumeName).setOwner("test");
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals("test", volume.getOwner());
  }

  @Test
  public void testSetVolumeQuota()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    store.getVolume(volumeName).setQuota(
        OzoneQuota.parseQuota("100000000 BYTES"));
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(100000000L, volume.getQuota());
  }

  @Test
  public void testDeleteVolume()
      throws IOException, OzoneException {
    thrown.expectMessage("Info Volume failed, error");
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertNotNull(volume);
    store.deleteVolume(volumeName);
    store.getVolume(volumeName);
  }

  @Test
  public void testCreateBucket()
      throws IOException, OzoneException {
    long currentTime = Time.now();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getCreationTime() >= currentTime);
    Assert.assertTrue(volume.getCreationTime() >= currentTime);
  }

  @Test
  public void testCreateBucketWithVersioning()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(true, bucket.getVersioning());
  }

  @Test
  public void testCreateBucketWithStorageType()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.SSD);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
  }

  @Test
  public void testCreateBucketWithAcls()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testCreateBucketWithAllArgument()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true)
        .setStorageType(StorageType.SSD)
        .setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(true, bucket.getVersioning());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testInvalidBucketCreation() throws IOException {
    thrown.expectMessage("Bucket or Volume name has an unsupported" +
        " character : #");
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "invalid#bucket";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
  }

  @Test
  public void testAddBucketAcl()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(new OzoneAcl(
        OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE));
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.addAcls(acls);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertTrue(bucket.getAcls().contains(acls.get(0)));
  }

  @Test
  public void testRemoveBucketAcl()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.removeAcls(acls);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertTrue(!bucket.getAcls().contains(acls.get(0)));
  }

  @Test
  public void testSetBucketVersioning()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setVersioning(true);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertEquals(true, newBucket.getVersioning());
  }

  @Test
  public void testSetBucketStorageType()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setStorageType(StorageType.SSD);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertEquals(StorageType.SSD, newBucket.getStorageType());
  }


  @Test
  public void testDeleteBucket()
      throws IOException, OzoneException {
    thrown.expectMessage("Info Bucket failed, error");
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertNotNull(bucket);
    volume.deleteBucket(bucketName);
    volume.getBucket(bucketName);
  }

  private boolean verifyRatisReplication(String volumeName, String bucketName,
      String keyName, ReplicationType type, ReplicationFactor factor)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    HddsProtos.ReplicationType replicationType =
        HddsProtos.ReplicationType.valueOf(type.toString());
    HddsProtos.ReplicationFactor replicationFactor =
        HddsProtos.ReplicationFactor.valueOf(factor.getValue());
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info:
        keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container =
          storageContainerLocationClient.getContainer(info.getContainerID());
      if (!container.getReplicationFactor().equals(replicationFactor) || (
          container.getReplicationType() != replicationType)) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testPutKey()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes().length, ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE);
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }
  }

  @Test
  public void testValidateBlockLengthWithCommitKey() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(RandomUtils.nextInt(0, 1024));
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // create the initial key with size 0, write will allocate the first block.
    OzoneOutputStream out = bucket.createKey(keyName, 0,
        ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
    out.write(value.getBytes());
    out.close();
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(builder.build());

    List<OmKeyLocationInfo> locationInfoList =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    // LocationList should have only 1 block
    Assert.assertEquals(1, locationInfoList.size());
    // make sure the data block size is updated
    Assert.assertEquals(value.getBytes().length,
        locationInfoList.get(0).getLength());
    // make sure the total data size is set correctly
    Assert.assertEquals(value.getBytes().length, keyInfo.getDataSize());
  }


  @Test
  public void testPutKeyRatisOneNode()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes().length, ReplicationType.RATIS,
          ReplicationFactor.ONE);
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      is.close();
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS, ReplicationFactor.ONE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }
  }

  @Test
  public void testPutKeyRatisThreeNodes()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes().length, ReplicationType.RATIS,
          ReplicationFactor.THREE);
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      is.close();
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS,
          ReplicationFactor.THREE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }
  }

  @Test
  public void testGetKeyDetails() throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();
    String keyValue = RandomStringUtils.random(128);
    //String keyValue = "this is a test value.glx";
    // create the initial key with size 0, write will allocate the first block.
    OzoneOutputStream out = bucket.createKey(keyName,
        keyValue.getBytes().length, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE);
    out.write(keyValue.getBytes());
    out.close();

    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[32];
    is.read(fileContent);

    // First, confirm the key info from the client matches the info in OM.
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName);
    OmKeyLocationInfo keyInfo = ozoneManager.lookupKey(builder.build()).
        getKeyLocationVersions().get(0).getBlocksLatestVersionOnly().get(0);
    long containerID = keyInfo.getContainerID();
    long localID = keyInfo.getLocalID();
    OzoneKeyDetails keyDetails = (OzoneKeyDetails)bucket.getKey(keyName);
    Assert.assertEquals(keyName, keyDetails.getName());

    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    Assert.assertEquals(1, keyLocations.size());
    Assert.assertEquals(containerID, keyLocations.get(0).getContainerID());
    Assert.assertEquals(localID, keyLocations.get(0).getLocalID());

    // Make sure that the data size matched.
    Assert.assertEquals(keyValue.getBytes().length,
        keyLocations.get(0).getLength());

    // Second, sum the data size from chunks in Container via containerID
    // and localID, make sure the size equals to the size from keyDetails.
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getContainerManager().getContainerWithPipeline(containerID)
        .getPipeline();
    List<DatanodeDetails> datanodes = pipeline.getMachines();
    Assert.assertEquals(datanodes.size(), 1);

    DatanodeDetails datanodeDetails = datanodes.get(0);
    Assert.assertNotNull(datanodeDetails);
    HddsDatanodeService datanodeService = null;
    for (HddsDatanodeService datanodeServiceItr : cluster.getHddsDatanodes()) {
      if (datanodeDetails.equals(datanodeServiceItr.getDatanodeDetails())) {
        datanodeService = datanodeServiceItr;
        break;
      }
    }
    KeyValueContainerData containerData =
        (KeyValueContainerData)(datanodeService.getDatanodeStateMachine()
        .getContainer().getContainerSet().getContainer(containerID)
        .getContainerData());
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath));
    while (keyValueBlockIterator.hasNext()) {
      BlockData blockData = keyValueBlockIterator.nextBlock();
      if (blockData.getBlockID().getLocalID() == localID) {
        long length = 0;
        List<ContainerProtos.ChunkInfo> chunks = blockData.getChunks();
        for (ContainerProtos.ChunkInfo chunk : chunks) {
          length += chunk.getLen();
        }
        Assert.assertEquals(length, keyValue.getBytes().length);
        break;
      }
    }
  }

  @Test
  public void testDeleteKey()
      throws IOException, OzoneException {
    thrown.expectMessage("Lookup key failed, error");
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE);
    out.write(value.getBytes());
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    bucket.deleteKey(keyName);
    bucket.getKey(keyName);
  }

  @Test
  public void testRenameKey()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String fromKeyName = UUID.randomUUID().toString();
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneOutputStream out = bucket.createKey(fromKeyName,
        value.getBytes().length, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE);
    out.write(value.getBytes());
    out.close();
    OzoneKey key = bucket.getKey(fromKeyName);
    Assert.assertEquals(fromKeyName, key.getName());

    // Rename to empty string should fail.
    IOException ioe = null;
    String toKeyName = "";
    try {
      bucket.renameKey(fromKeyName, toKeyName);
    } catch (IOException e) {
      ioe = e;
    }
    Assert.assertTrue(ioe.getMessage().contains("Rename key failed, error"));

    toKeyName = UUID.randomUUID().toString();
    bucket.renameKey(fromKeyName, toKeyName);

    // Lookup for old key should fail.
    try {
      bucket.getKey(fromKeyName);
    } catch (IOException e) {
      ioe = e;
    }
    Assert.assertTrue(ioe.getMessage().contains("Lookup key failed, error"));

    key = bucket.getKey(toKeyName);
    Assert.assertEquals(toKeyName, key.getName());
  }

  // Listing all volumes in the cluster feature has to be fixed after HDDS-357.
  // TODO: fix this
  @Ignore
  @Test
  public void testListVolume() throws IOException, OzoneException {
    String volBase = "vol-" + RandomStringUtils.randomNumeric(3);
    //Create 10 volume vol-<random>-a-0-<random> to vol-<random>-a-9-<random>
    String volBaseNameA = volBase + "-a-";
    for(int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    //Create 10 volume vol-<random>-b-0-<random> to vol-<random>-b-9-<random>
    String volBaseNameB = volBase + "-b-";
    for(int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    Iterator<? extends OzoneVolume> volIterator = store.listVolumes(volBase);
    int totalVolumeCount = 0;
    while(volIterator.hasNext()) {
      volIterator.next();
      totalVolumeCount++;
    }
    Assert.assertEquals(20, totalVolumeCount);
    Iterator<? extends OzoneVolume> volAIterator = store.listVolumes(
        volBaseNameA);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volAIterator.next().getName()
          .startsWith(volBaseNameA + i + "-"));
    }
    Assert.assertFalse(volAIterator.hasNext());
    Iterator<? extends OzoneVolume> volBIterator = store.listVolumes(
        volBaseNameB);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBIterator.next().getName()
          .startsWith(volBaseNameB + i + "-"));
    }
    Assert.assertFalse(volBIterator.hasNext());
    Iterator<? extends OzoneVolume> iter = store.listVolumes(volBaseNameA +
        "1-");
    Assert.assertTrue(iter.next().getName().startsWith(volBaseNameA + "1-"));
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testListBucket()
      throws IOException, OzoneException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);

    //Create 10 buckets in  vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameA = "bucket-a-";
    for(int i = 0; i < 10; i++) {
      volA.createBucket(
          bucketBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
      volB.createBucket(
          bucketBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    //Create 10 buckets in vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameB = "bucket-b-";
    for(int i = 0; i < 10; i++) {
      volA.createBucket(
          bucketBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
      volB.createBucket(
          bucketBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    Iterator<? extends OzoneBucket> volABucketIter =
        volA.listBuckets("bucket-");
    int volABucketCount = 0;
    while(volABucketIter.hasNext()) {
      volABucketIter.next();
      volABucketCount++;
    }
    Assert.assertEquals(20, volABucketCount);
    Iterator<? extends OzoneBucket> volBBucketIter =
        volA.listBuckets("bucket-");
    int volBBucketCount = 0;
    while(volBBucketIter.hasNext()) {
      volBBucketIter.next();
      volBBucketCount++;
    }
    Assert.assertEquals(20, volBBucketCount);

    Iterator<? extends OzoneBucket> volABucketAIter =
        volA.listBuckets("bucket-a-");
    int volABucketACount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketACount++;
    }
    Assert.assertEquals(10, volABucketACount);
    Iterator<? extends OzoneBucket> volBBucketBIter =
        volA.listBuckets("bucket-b-");
    int volBBucketBCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBCount++;
    }
    Assert.assertEquals(10, volBBucketBCount);
    Iterator<? extends OzoneBucket> volABucketBIter = volA.listBuckets(
        "bucket-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketBIter.next().getName()
          .startsWith(bucketBaseNameB + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
    Iterator<? extends OzoneBucket> volBBucketAIter = volB.listBuckets(
        "bucket-a-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBBucketAIter.next().getName()
          .startsWith(bucketBaseNameA + i + "-"));
    }
    Assert.assertFalse(volBBucketAIter.hasNext());

  }

  @Test
  public void testListBucketsOnEmptyVolume()
      throws IOException, OzoneException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    Iterator<? extends OzoneBucket> buckets = vol.listBuckets("");
    while(buckets.hasNext()) {
      Assert.fail();
    }
  }

  @Test
  public void testListKey()
      throws IOException, OzoneException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    volB.createBucket(bucketA);
    volB.createBucket(bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);
    OzoneBucket volBbucketA = volB.getBucket(bucketA);
    OzoneBucket volBbucketB = volB.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes();
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      four.write(value);
      four.close();
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes();
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      four.write(value);
      four.close();
    }
    Iterator<? extends OzoneKey> volABucketAIter =
        volAbucketA.listKeys("key-");
    int volABucketAKeyCount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    Assert.assertEquals(20, volABucketAKeyCount);
    Iterator<? extends OzoneKey> volABucketBIter =
        volAbucketB.listKeys("key-");
    int volABucketBKeyCount = 0;
    while(volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    Assert.assertEquals(20, volABucketBKeyCount);
    Iterator<? extends OzoneKey> volBBucketAIter =
        volBbucketA.listKeys("key-");
    int volBBucketAKeyCount = 0;
    while(volBBucketAIter.hasNext()) {
      volBBucketAIter.next();
      volBBucketAKeyCount++;
    }
    Assert.assertEquals(20, volBBucketAKeyCount);
    Iterator<? extends OzoneKey> volBBucketBIter =
        volBbucketB.listKeys("key-");
    int volBBucketBKeyCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBKeyCount++;
    }
    Assert.assertEquals(20, volBBucketBKeyCount);
    Iterator<? extends OzoneKey> volABucketAKeyAIter =
        volAbucketA.listKeys("key-a-");
    int volABucketAKeyACount = 0;
    while(volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    Assert.assertEquals(10, volABucketAKeyACount);
    Iterator<? extends OzoneKey> volABucketAKeyBIter =
        volAbucketA.listKeys("key-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketAKeyBIter.next().getName()
          .startsWith("key-b-" + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
  }

  @Test
  public void testListKeyOnEmptyBucket()
      throws IOException, OzoneException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<? extends OzoneKey> keys = buc.listKeys("");
    while(keys.hasNext()) {
      Assert.fail();
    }
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    if(ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
