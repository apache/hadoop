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

package org.apache.hadoop.fs.ozone;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ratis.util.LifeCycle;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsUtils.getHostName;
import static org.apache.hadoop.hdds.HddsUtils.getHostPort;

/**
 * Test client-side URI handling with Ozone Manager HA.
 */
public class TestOzoneFsHAURLs {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestOzoneFsHAURLs.class);

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private String omId;
  private String omServiceId;
  private String clusterId;
  private String scmId;
  private OzoneManager om;
  private int numOfOMs;

  private String volumeName;
  private String bucketName;
  private String rootPath;

  private final String o3fsImplKey =
      "fs." + OzoneConsts.OZONE_URI_SCHEME + ".impl";
  private final String o3fsImplValue =
      "org.apache.hadoop.fs.ozone.OzoneFileSystem";

  private static final long LEADER_ELECTION_TIMEOUT = 500L;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    omId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    final String path = GenericTestUtils.getTempPath(omId);
    java.nio.file.Path metaDirPath = java.nio.file.Paths.get(path, "om-meta");
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.setTimeDuration(
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        LEADER_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT, 3);

    OMStorage omStore = new OMStorage(conf);
    omStore.setClusterId(clusterId);
    omStore.setScmId(scmId);
    // writes the version file properties
    omStore.initialize();

    // Start the cluster
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setNumDatanodes(7)
        .setPipelineNumber(10)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();

    om = cluster.getOzoneManager();
    Assert.assertEquals(LifeCycle.State.RUNNING, om.getOmRatisServerState());

    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    ObjectStore objectStore =
        OzoneClientFactory.getRpcClient(omServiceId, conf).getObjectStore();
    objectStore.createVolume(volumeName);

    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);
    bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    retVolumeinfo.createBucket(bucketName);

    rootPath = String.format("%s://%s.%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
        bucketName, volumeName, omServiceId);
    // Set fs.defaultFS
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem fs = FileSystem.get(conf);
    // Create some dirs
    Path root = new Path("/");
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * @return the leader OM's RPC address in the MiniOzoneHACluster
   */
  private String getLeaderOMNodeAddr() {
    String leaderOMNodeAddr = null;
    Collection<String> omNodeIds = OmUtils.getOMNodeIds(conf, omServiceId);
    assert(omNodeIds.size() == numOfOMs);
    MiniOzoneHAClusterImpl haCluster = (MiniOzoneHAClusterImpl) cluster;
    // Note: this loop may be implemented inside MiniOzoneHAClusterImpl
    for (String omNodeId : omNodeIds) {
      // Find the leader OM
      if (!haCluster.getOzoneManager(omNodeId).isLeader()) {
        continue;
      }
      // ozone.om.address.omServiceId.omNode
      String leaderOMNodeAddrKey = OmUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodeId);
      leaderOMNodeAddr = conf.get(leaderOMNodeAddrKey);
      LOG.info("Found leader OM: nodeId=" + omNodeId + ", " +
          leaderOMNodeAddrKey + "=" + leaderOMNodeAddr);
      // Leader found, no need to continue loop
      break;
    }
    // There has to be a leader
    assert(leaderOMNodeAddr != null);
    return leaderOMNodeAddr;
  }

  /**
   * Get host name from an address. This uses getHostName() internally.
   * @param addr Address with port number
   * @return Host name
   */
  private String getHostFromAddress(String addr) {
    Optional<String> hostOptional = getHostName(addr);
    assert(hostOptional.isPresent());
    return hostOptional.get();
  }

  /**
   * Get port number from an address. This uses getHostPort() internally.
   * @param addr Address with port
   * @return Port number
   */
  private int getPortFromAddress(String addr) {
    Optional<Integer> portOptional = getHostPort(addr);
    assert(portOptional.isPresent());
    return portOptional.get();
  }

  /**
   * Test OM HA URLs with qualified fs.defaultFS.
   * @throws Exception
   */
  @Test
  public void testWithQualifiedDefaultFS() throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    clientConf.setQuietMode(false);
    clientConf.set(o3fsImplKey, o3fsImplValue);
    // fs.defaultFS = o3fs://bucketName.volumeName.omServiceId/
    clientConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    // Pick leader OM's RPC address and assign it to ozone.om.address for
    // the test case: ozone fs -ls o3fs://bucket.volume.om1/
    String leaderOMNodeAddr = getLeaderOMNodeAddr();
    // ozone.om.address was set to service id in MiniOzoneHAClusterImpl
    clientConf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, leaderOMNodeAddr);

    FsShell shell = new FsShell(clientConf);
    int res;
    try {
      // Test case 1: ozone fs -ls /
      // Expectation: Success.
      res = ToolRunner.run(shell, new String[] {"-ls", "/"});
      // Check return value, should be 0 (success)
      Assert.assertEquals(res, 0);

      // Test case 2: ozone fs -ls o3fs:///
      // Expectation: Success. fs.defaultFS is a fully qualified path.
      res = ToolRunner.run(shell, new String[] {"-ls", "o3fs:///"});
      Assert.assertEquals(res, 0);

      // Test case 3: ozone fs -ls o3fs://bucket.volume/
      // Expectation: Fail. Must have service id or host name when HA is enabled
      String unqualifiedPath1 = String.format("%s://%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);
      try (GenericTestUtils.SystemErrCapturer capture =
          new GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell, new String[] {"-ls", unqualifiedPath1});
        // Check stderr, inspired by testDFSWithInvalidCommmand
        Assert.assertThat("Command did not print the error message " +
                "correctly for test case: ozone fs -ls o3fs://bucket.volume/",
            capture.getOutput(), StringContains.containsString(
                "-ls: Service ID or host name must not"
                    + " be omitted when ozone.om.service.ids is defined."));
      }
      // Check return value, should be -1 (failure)
      Assert.assertEquals(res, -1);

      // Test case 4: ozone fs -ls o3fs://bucket.volume.om1/
      // Expectation: Success. The client should use the port number
      // set in ozone.om.address.
      String qualifiedPath1 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          getHostFromAddress(leaderOMNodeAddr));
      res = ToolRunner.run(shell, new String[] {"-ls", qualifiedPath1});
      // Note: this test case will fail if the port is not from the leader node
      Assert.assertEquals(res, 0);

      // Test case 5: ozone fs -ls o3fs://bucket.volume.om1:port/
      // Expectation: Success.
      String qualifiedPath2 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          leaderOMNodeAddr);
      res = ToolRunner.run(shell, new String[] {"-ls", qualifiedPath2});
      Assert.assertEquals(res, 0);

      // Test case 6: ozone fs -ls o3fs://bucket.volume.id1/
      // Expectation: Success.
      String qualifiedPath3 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName, omServiceId);
      res = ToolRunner.run(shell, new String[] {"-ls", qualifiedPath3});
      Assert.assertEquals(res, 0);

      // Test case 7: ozone fs -ls o3fs://bucket.volume.id1:port/
      // Expectation: Fail. Service ID does not use port information.
      // Use the port number from leader OM (doesn't really matter)
      String unqualifiedPath2 = String.format("%s://%s.%s.%s:%d/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          omServiceId, getPortFromAddress(leaderOMNodeAddr));
      try (GenericTestUtils.SystemErrCapturer capture =
          new GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell, new String[] {"-ls", unqualifiedPath2});
        // Check stderr
        Assert.assertThat("Command did not print the error message " +
                "correctly for test case: "
                + "ozone fs -ls o3fs://bucket.volume.id1:port/",
            capture.getOutput(), StringContains.containsString(
                "does not use port information"));
      }
      // Check return value, should be -1 (failure)
      Assert.assertEquals(res, -1);
    } finally {
      shell.close();
    }
  }

  /**
   * Helper function for testOtherDefaultFS(),
   * run fs -ls o3fs:/// against different fs.defaultFS input.
   *
   * @param defaultFS Desired fs.defaultFS to be used in the test
   * @throws Exception
   */
  private void testWithDefaultFS(String defaultFS) throws Exception {
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    clientConf.setQuietMode(false);
    clientConf.set(o3fsImplKey, o3fsImplValue);
    // fs.defaultFS = file:///
    clientConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        defaultFS);

    FsShell shell = new FsShell(clientConf);
    try {
      // Test case: ozone fs -ls o3fs:///
      // Expectation: Fail. fs.defaultFS is not a qualified o3fs URI.
      int res = ToolRunner.run(shell, new String[] {"-ls", "o3fs:///"});
      Assert.assertEquals(res, -1);
    } finally {
      shell.close();
    }
  }

  /**
   * Test OM HA URLs with some unqualified fs.defaultFS.
   * @throws Exception
   */
  @Test
  public void testOtherDefaultFS() throws Exception {
    // Test scenarios where fs.defaultFS isn't a fully qualified o3fs

    // fs.defaultFS = file:///
    testWithDefaultFS(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);

    // fs.defaultFS = hdfs://ns1/
    testWithDefaultFS("hdfs://ns1/");

    // fs.defaultFS = o3fs:///
    String unqualifiedFs1 = String.format(
        "%s:///", OzoneConsts.OZONE_URI_SCHEME);
    testWithDefaultFS(unqualifiedFs1);

    // fs.defaultFS = o3fs://bucketName.volumeName/
    String unqualifiedFs2 = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);
    testWithDefaultFS(unqualifiedFs2);
  }
}
