/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests Close Container Exception handling by Ozone Client.
 */
public class TestCloseContainerHandlingByClient {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static int chunkSize;
  private static int blockSize;
  private static String volumeName;
  private static String bucketName;
  private static String keyString;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    chunkSize = (int) OzoneConsts.MB;
    blockSize = 4 * chunkSize;
    conf.set(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, "5000ms");
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE, "NONE");
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(7).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "closecontainerexceptionhandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockWritesWithFlushAndClose() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    // write data more than 1 chunk
    byte[] data = ContainerTestHelper
        .getFixedLengthString(keyString, chunkSize + chunkSize / 2)
        .getBytes(UTF_8);
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();

    waitForContainerClose(key);
    key.write(data);
    key.flush();
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    Assert.assertEquals(2 * data.length, keyInfo.getDataSize());

    // Written the same data twice
    String dataString = new String(data, UTF_8);
    dataString = dataString.concat(dataString);
    validateData(keyName, dataString.getBytes(UTF_8));
  }

  @Test
  public void testBlockWritesCloseConsistency() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    // write data more than 1 chunk
    byte[] data = ContainerTestHelper
        .getFixedLengthString(keyString, chunkSize + chunkSize / 2)
        .getBytes(UTF_8);
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();

    waitForContainerClose(key);
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    Assert.assertEquals(data.length, keyInfo.getDataSize());
    validateData(keyName, data);
  }

  @Test
  public void testMultiBlockWrites() throws Exception {

    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, (3 * blockSize));
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();
    // With the initial size provided, it should have preallocated 4 blocks
    Assert.assertEquals(3, keyOutputStream.getStreamEntries().size());
    // write data more than 1 block
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, (3 * blockSize))
            .getBytes(UTF_8);
    Assert.assertEquals(data.length, 3 * blockSize);
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();

    waitForContainerClose(key);
    // write 1 more block worth of data. It will fail and new block will be
    // allocated
    key.write(ContainerTestHelper.getFixedLengthString(keyString, blockSize)
        .getBytes(UTF_8));

    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    // Though we have written only block initially, the close will hit
    // closeContainerException and remaining data in the chunkOutputStream
    // buffer will be copied into a different allocated block and will be
    // committed.
    Assert.assertEquals(4, keyLocationInfos.size());
    Assert.assertEquals(4 * blockSize, keyInfo.getDataSize());
    for (OmKeyLocationInfo locationInfo : keyLocationInfos) {
      Assert.assertEquals(blockSize, locationInfo.getLength());
    }
  }

  @Test
  public void testMultiBlockWrites2() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 2 * blockSize);
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    // With the initial size provided, it should have pre allocated 2 blocks
    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    String dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (2 * blockSize));
    byte[] data = dataString.getBytes(UTF_8);
    key.write(data);
    // 2 block are completely written to the DataNode in 3 blocks.
    // Data of length half of chunkSize resides in the chunkOutput stream buffer
    String dataString2 =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize);
    key.write(dataString2.getBytes(UTF_8));
    key.flush();

    String dataString3 =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize);
    key.write(dataString3.getBytes(UTF_8));
    key.flush();

    String dataString4 =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize * 1 / 2);
    key.write(dataString4.getBytes(UTF_8));
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();

    waitForContainerClose(key);

    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    // Though we have written only block initially, the close will hit
    // closeContainerException and remaining data in the chunkOutputStream
    // buffer will be copied into a different allocated block and will be
    // committed.

    String dataCommitted =
        dataString.concat(dataString2).concat(dataString3).concat(dataString4);
    Assert.assertEquals(dataCommitted.getBytes(UTF_8).length,
        keyInfo.getDataSize());
    validateData(keyName, dataCommitted.getBytes(UTF_8));
  }

  @Test
  public void testMultiBlockWrites3() throws Exception {

    String keyName = getKeyName();
    int keyLen = 4 * blockSize;
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, keyLen);
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();
    // With the initial size provided, it should have preallocated 4 blocks
    Assert.assertEquals(4, keyOutputStream.getStreamEntries().size());
    // write data 3 blocks and one more chunk
    byte[] writtenData =
        ContainerTestHelper.getFixedLengthString(keyString, keyLen)
            .getBytes(UTF_8);
    byte[] data = Arrays.copyOfRange(writtenData, 0, 3 * blockSize + chunkSize);
    Assert.assertEquals(data.length, 3 * blockSize + chunkSize);
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();

    waitForContainerClose(key);
    // write 3 more chunks worth of data. It will fail and new block will be
    // allocated. This write completes 4 blocks worth of data written to key
    data = Arrays.copyOfRange(writtenData, 3 * blockSize + chunkSize, keyLen);
    key.write(data);

    key.close();
    // read the key from OM again and match the length and data.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneInputStream inputStream = bucket.readKey(keyName);
    byte[] readData = new byte[keyLen];
    inputStream.read(readData);
    Assert.assertArrayEquals(writtenData, readData);

    // Though we have written only block initially, the close will hit
    // closeContainerException and remaining data in the chunkOutputStream
    // buffer will be copied into a different allocated block and will be
    // committed.
    long length = 0;
    for (OmKeyLocationInfo locationInfo : keyLocationInfos) {
      length += locationInfo.getLength();
    }
    Assert.assertEquals(4 * blockSize, length);
  }

  private void waitForContainerClose(OzoneOutputStream outputStream)
      throws Exception {
    ContainerTestHelper
        .waitForContainerClose(outputStream, cluster);
  }

  @Ignore // test needs to be fixed after close container is handled for
  // non-existent containers on datanode. Test closes pre allocated containers
  // on the datanode.
  @Test
  public void testDiscardPreallocatedBlocks() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 2 * blockSize);
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    // With the initial size provided, it should have pre allocated 4 blocks
    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    String dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (1 * blockSize));
    byte[] data = dataString.getBytes(UTF_8);
    key.write(data);
    List<OmKeyLocationInfo> locationInfos =
        new ArrayList<>(keyOutputStream.getLocationInfoList());
    long containerID = locationInfos.get(0).getContainerID();
    ContainerInfo container =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainer(ContainerID.valueof(containerID));
    Pipeline pipeline =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    Assert.assertEquals(1, datanodes.size());
    waitForContainerClose(key);
    dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (1 * blockSize));
    data = dataString.getBytes(UTF_8);
    key.write(data);
    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());

    // the 1st block got written. Now all the containers are closed, so the 2nd
    // pre allocated block will be removed from the list and new block should
    // have been allocated
    Assert.assertTrue(
        keyOutputStream.getLocationInfoList().get(0).getBlockID()
            .equals(locationInfos.get(0).getBlockID()));
    Assert.assertFalse(
        keyOutputStream.getLocationInfoList().get(1).getBlockID()
            .equals(locationInfos.get(1).getBlockID()));
    key.close();
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return ContainerTestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }

  private void validateData(String keyName, byte[] data) throws Exception {
    ContainerTestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }

  @Test
  public void testBlockWriteViaRatis() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    byte[] data = ContainerTestHelper
        .getFixedLengthString(keyString, chunkSize + chunkSize / 2)
        .getBytes(UTF_8);
    key.write(data);

    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).
        setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    waitForContainerClose(key);
    // Again Write the Data. This will throw an exception which will be handled
    // and new blocks will be allocated
    key.write(data);
    key.flush();
    // The write will fail but exception will be handled and length will be
    // updated correctly in OzoneManager once the steam is closed
    key.close();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    String dataString = new String(data, UTF_8);
    dataString = dataString.concat(dataString);
    Assert.assertEquals(2 * data.length, keyInfo.getDataSize());
    validateData(keyName, dataString.getBytes(UTF_8));
  }

  @Test
  public void testBlockWrites() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, 2 * chunkSize)
            .getBytes(UTF_8);
    key.write(data1);

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();

    waitForContainerClose(key);
    byte[] data2 =
        ContainerTestHelper.getFixedLengthString(keyString, 3 * chunkSize)
            .getBytes(UTF_8);
    key.write(data2);
    key.flush();
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    Assert.assertEquals(5 * chunkSize, keyInfo.getDataSize());

    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    // Written the same data twice
    String dataString2 = new String(data2, UTF_8);
    dataString = dataString.concat(dataString2);
    validateData(keyName, dataString.getBytes(UTF_8));
  }

}
