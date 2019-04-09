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
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl
    .NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CLIENT_RETRY_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
public class TestOzoneManagerHA {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private int numOfOMs = 3;
  private static final long SNAPSHOT_THRESHOLD = 50;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(300_000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.setInt(OZONE_CLIENT_RETRY_MAX_ATTEMPTS_KEY, 10);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 10);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(conf).getObjectStore();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  private OzoneVolume createAndCheckVolume(String volumeName)
      throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);

    OzoneVolume retVolume = objectStore.getVolume(volumeName);

    Assert.assertTrue(retVolume.getName().equals(volumeName));
    Assert.assertTrue(retVolume.getOwner().equals(userName));
    Assert.assertTrue(retVolume.getAdmin().equals(adminName));

    return retVolume;
  }
  @Test
  public void testAllVolumeOperations() throws Exception {

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    createAndCheckVolume(volumeName);

    objectStore.deleteVolume(volumeName);

    OzoneTestUtils.expectOmException(OMException.ResultCodes.VOLUME_NOT_FOUND,
        () -> objectStore.getVolume(volumeName));

    OzoneTestUtils.expectOmException(OMException.ResultCodes.VOLUME_NOT_FOUND,
        () -> objectStore.deleteVolume(volumeName));
  }


  @Test
  public void testAllBucketOperations() throws Exception {

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "volume" + RandomStringUtils.randomNumeric(5);

    OzoneVolume retVolume = createAndCheckVolume(volumeName);

    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setStorageType(StorageType.DISK)
            .setVersioning(true).build();


    retVolume.createBucket(bucketName, bucketArgs);


    OzoneBucket ozoneBucket = retVolume.getBucket(bucketName);

    Assert.assertEquals(volumeName, ozoneBucket.getVolumeName());
    Assert.assertEquals(bucketName, ozoneBucket.getName());
    Assert.assertTrue(ozoneBucket.getVersioning());
    Assert.assertEquals(StorageType.DISK, ozoneBucket.getStorageType());
    Assert.assertTrue(ozoneBucket.getCreationTime() <= Time.now());


    // Change versioning to false
    ozoneBucket.setVersioning(false);

    ozoneBucket = retVolume.getBucket(bucketName);
    Assert.assertFalse(ozoneBucket.getVersioning());

    retVolume.deleteBucket(bucketName);

    OzoneTestUtils.expectOmException(OMException.ResultCodes.BUCKET_NOT_FOUND,
        () -> retVolume.deleteBucket(bucketName));



  }

  /**
   * Test a client request when all OM nodes are running. The request should
   * succeed.
   * @throws Exception
   */
  @Test
  public void testAllOMNodesRunning() throws Exception {
    createVolumeTest(true);
    createKeyTest(true);
  }

  /**
   * Test client request succeeds even if one OM is down.
   */
  @Test
  public void testOneOMNodeDown() throws Exception {
    cluster.stopOzoneManager(1);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    createVolumeTest(true);

    createKeyTest(true);
  }

  /**
   * Test client request fails when 2 OMs are down.
   */
  @Test
  public void testTwoOMNodesDown() throws Exception {
    cluster.stopOzoneManager(1);
    cluster.stopOzoneManager(2);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    createVolumeTest(false);

    createKeyTest(false);

  }

  private OzoneBucket setupBucket() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
    Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
    Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));

    String bucketName = UUID.randomUUID().toString();
    retVolumeinfo.createBucket(bucketName);

    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));
    Assert.assertTrue(ozoneBucket.getVolumeName().equals(volumeName));

    return ozoneBucket;
  }

  @Test
  public void testMultipartUpload() throws Exception {

    // Happy scenario when all OM's are up.
    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

  }

  @Test
  public void testMultipartUploadWithOneOmNodeDown() throws Exception {

    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    // After initiate multipartupload, shutdown leader OM.
    // Stop leader OM, to see when the OM leader changes
    // multipart upload is happening successfully or not.

    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    cluster.stopOzoneManager(leaderOMNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    Assert.assertTrue(leaderOMNodeId != newLeaderOMNodeId);
  }


  private String initiateMultipartUpload(OzoneBucket ozoneBucket,
      String keyName) throws Exception {

    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName,
            ReplicationType.RATIS,
            ReplicationFactor.ONE);

    String uploadID = omMultipartInfo.getUploadID();
    Assert.assertTrue(uploadID != null);
    return uploadID;
  }

  private void createMultipartKeyAndReadKey(OzoneBucket ozoneBucket,
      String keyName, String uploadID) throws Exception {

    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), 1, uploadID);
    ozoneOutputStream.write(value.getBytes(), 0, value.length());
    ozoneOutputStream.close();


    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, ozoneOutputStream.getCommitUploadPartInfo().getPartName());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        ozoneBucket.completeMultipartUpload(keyName, uploadID, partsMap);

    Assert.assertTrue(omMultipartUploadCompleteInfo != null);
    Assert.assertTrue(omMultipartUploadCompleteInfo.getHash() != null);


    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName);

    byte[] fileContent = new byte[value.getBytes().length];
    ozoneInputStream.read(fileContent);
    Assert.assertEquals(value, new String(fileContent));
  }


  private void createKeyTest(boolean checkSuccess) throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    try {
      objectStore.createVolume(volumeName, createVolumeArgs);

      OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

      Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
      Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
      Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));

      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();
      retVolumeinfo.createBucket(bucketName);

      OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

      Assert.assertTrue(ozoneBucket.getName().equals(bucketName));
      Assert.assertTrue(ozoneBucket.getVolumeName().equals(volumeName));

      String value = "random data";
      OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
          value.length(), ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE, new HashMap<>());
      ozoneOutputStream.write(value.getBytes(), 0, value.length());
      ozoneOutputStream.close();

      OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName);

      byte[] fileContent = new byte[value.getBytes().length];
      ozoneInputStream.read(fileContent);
      Assert.assertEquals(value, new String(fileContent));

    } catch (ConnectException | RemoteException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          GenericTestUtils.assertExceptionContains(
              "RaftRetryFailureException", e);
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Create a volume and test its attribute.
   */
  private void createVolumeTest(boolean checkSuccess) throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    try {
      objectStore.createVolume(volumeName, createVolumeArgs);

      OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

      if (checkSuccess) {
        Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
        Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
        Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));
      } else {
        // Verify that the request failed
        Assert.fail("There is no quorum. Request should have failed");
      }
    } catch (ConnectException | RemoteException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          GenericTestUtils.assertExceptionContains(
              "RaftRetryFailureException", e);
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Test that OMFailoverProxyProvider creates an OM proxy for each OM in the
   * cluster.
   */
  @Test
  public void testOMProxyProviderInitialization() throws Exception {
    OzoneClient rpcClient = cluster.getRpcClient();
    OMFailoverProxyProvider omFailoverProxyProvider =
        rpcClient.getObjectStore().getClientProxy().getOMProxyProvider();
    List<OMFailoverProxyProvider.OMProxyInfo> omProxies =
        omFailoverProxyProvider.getOMProxies();

    Assert.assertEquals(numOfOMs, omProxies.size());

    for (int i = 0; i < numOfOMs; i++) {
      InetSocketAddress omRpcServerAddr =
          cluster.getOzoneManager(i).getOmRpcServerAddr();
      boolean omClientProxyExists = false;
      for (OMFailoverProxyProvider.OMProxyInfo omProxyInfo : omProxies) {
        if (omProxyInfo.getAddress().equals(omRpcServerAddr)) {
          omClientProxyExists = true;
          break;
        }
      }
      Assert.assertTrue("There is no OM Client Proxy corresponding to OM " +
              "node" + cluster.getOzoneManager(i).getOMNodId(),
          omClientProxyExists);
    }
  }

  /**
   * Test OMFailoverProxyProvider failover on connection exception to OM client.
   */
  @Test
  public void testOMProxyProviderFailoverOnConnectionFailure()
      throws Exception {
    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();
    String firstProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    createVolumeTest(true);

    // On stopping the current OM Proxy, the next connection attempt should
    // failover to a another OM proxy.
    cluster.stopOzoneManager(firstProxyNodeId);
    Thread.sleep(OZONE_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_DEFAULT * 4);

    // Next request to the proxy provider should result in a failover
    createVolumeTest(true);
    Thread.sleep(OZONE_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_DEFAULT);

    // Get the new OM Proxy NodeId
    String newProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Verify that a failover occured. the new proxy nodeId should be
    // different from the old proxy nodeId.
    Assert.assertNotEquals("Failover did not occur as expected",
        firstProxyNodeId, newProxyNodeId);
  }

  /**
   * Test OMFailoverProxyProvider failover when current OM proxy is not
   * the current OM Leader.
   */
  @Test
  public void testOMProxyProviderFailoverToCurrentLeader() throws Exception {
    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();

    // Run couple of createVolume tests to discover the current Leader OM
    createVolumeTest(true);
    createVolumeTest(true);

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Perform a manual failover of the proxy provider to move the
    // currentProxyIndex to a node other than the leader OM.
    omFailoverProxyProvider.performFailover(
        omFailoverProxyProvider.getProxy().proxy);

    String newProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();
    Assert.assertNotEquals(leaderOMNodeId, newProxyNodeId);

    // Once another request is sent to this new proxy node, the leader
    // information must be returned via the response and a failover must
    // happen to the leader proxy node.
    createVolumeTest(true);
    Thread.sleep(2000);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // The old and new Leader OM NodeId must match since there was no new
    // election in the Ratis ring.
    Assert.assertEquals(leaderOMNodeId, newLeaderOMNodeId);
  }

  @Test
  public void testOMRetryProxy() throws Exception {
    // Stop all the OMs. After making 5 (set maxRetries value) attempts at
    // connection, the RpcClient should give up.
    for (int i = 0; i < numOfOMs; i++) {
      cluster.stopOzoneManager(i);
    }

    final LogVerificationAppender appender = new LogVerificationAppender();
    final org.apache.log4j.Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);

    try {
      createVolumeTest(true);
      Assert.fail("TestOMRetryProxy should fail when there are no OMs running");
    } catch (ConnectException e) {
      // Each retry attempt tries upto 10 times to connect. So there should be
      // 10*10 "Retrying connect to server" messages
      Assert.assertEquals(100,
          appender.countLinesWithMessage("Retrying connect to server:"));

      Assert.assertEquals(1,
          appender.countLinesWithMessage("Failed to connect to OM. Attempted " +
              "10 retries and 10 failovers"));
    }
  }

  @Test
  public void testReadRequest() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    objectStore.createVolume(volumeName);

    OMFailoverProxyProvider omFailoverProxyProvider =
        objectStore.getClientProxy().getOMProxyProvider();
    String currentLeaderNodeId = omFailoverProxyProvider
        .getCurrentProxyOMNodeId();

    // A read request from any proxy should failover to the current leader OM
    for (int i = 0; i < numOfOMs; i++) {
      // Failover OMFailoverProxyProvider to OM at index i
      OzoneManager ozoneManager = cluster.getOzoneManager(i);
      String omHostName = ozoneManager.getOmRpcServerAddr().getHostName();
      int rpcPort = ozoneManager.getOmRpcServerAddr().getPort();

      // Get the ObjectStore and FailoverProxyProvider for OM at index i
      final ObjectStore store = OzoneClientFactory.getRpcClient(
          omHostName, rpcPort, conf).getObjectStore();
      final OMFailoverProxyProvider proxyProvider =
          store.getClientProxy().getOMProxyProvider();

      // Failover to the OM node that the objectStore points to
      omFailoverProxyProvider.performFailoverIfRequired(
          ozoneManager.getOMNodId());

      // A read request should result in the proxyProvider failing over to
      // leader node.
      OzoneVolume volume = store.getVolume(volumeName);
      Assert.assertEquals(volumeName, volume.getName());

      Assert.assertEquals(currentLeaderNodeId,
          proxyProvider.getCurrentProxyOMNodeId());
    }
  }

  @Test
  public void testOMRatisSnapshot() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName);
    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    String leaderOMNodeId = objectStore.getClientProxy().getOMProxyProvider()
        .getCurrentProxyOMNodeId();
    OzoneManager ozoneManager = cluster.getOzoneManager(leaderOMNodeId);

    // Send commands to ratis to increase the log index so that ratis
    // triggers a snapshot on the state machine.

    long appliedLogIndex = 0;
    while (appliedLogIndex <= SNAPSHOT_THRESHOLD) {
      createKey(ozoneBucket);
      appliedLogIndex = ozoneManager.getOmRatisServer()
          .getStateMachineLastAppliedIndex();
    }

    GenericTestUtils.waitFor(() -> {
      if (ozoneManager.loadRatisSnapshotIndex() > 0) {
        return true;
      }
      return false;
    }, 1000, 100000);

    // The current lastAppliedLogIndex on the state machine should be greater
    // than or equal to the saved snapshot index.
    long smLastAppliedIndex =
        ozoneManager.getOmRatisServer().getStateMachineLastAppliedIndex();
    long ratisSnapshotIndex = ozoneManager.loadRatisSnapshotIndex();
    Assert.assertTrue("LastAppliedIndex on OM State Machine ("
            + smLastAppliedIndex + ") is less than the saved snapshot index("
            + ratisSnapshotIndex + ").",
        smLastAppliedIndex >= ratisSnapshotIndex);

    // Add more transactions to Ratis to trigger another snapshot
    while (appliedLogIndex <= (smLastAppliedIndex + SNAPSHOT_THRESHOLD)) {
      createKey(ozoneBucket);
      appliedLogIndex = ozoneManager.getOmRatisServer()
          .getStateMachineLastAppliedIndex();
    }

    GenericTestUtils.waitFor(() -> {
      if (ozoneManager.loadRatisSnapshotIndex() > 0) {
        return true;
      }
      return false;
    }, 1000, 100000);

    // The new snapshot index must be greater than the previous snapshot index
    long ratisSnapshotIndexNew = ozoneManager.loadRatisSnapshotIndex();
    Assert.assertTrue("Latest snapshot index must be greater than previous " +
        "snapshot indices", ratisSnapshotIndexNew > ratisSnapshotIndex);

  }

  private void createKey(OzoneBucket ozoneBucket) throws IOException {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    String data = "data" + RandomStringUtils.randomNumeric(5);
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
        data.length(), ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream.write(data.getBytes(), 0, data.length());
    ozoneOutputStream.close();
  }
}
