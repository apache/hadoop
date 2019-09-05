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
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ratis.util.LifeCycle;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Test client-side URL handling with and without Ozone Manager HA.
 */
public class TestOzoneFsHAURLs {

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private String omId;
  private String omServiceId;
  private String clusterId;
  private String scmId;
  private OzoneManager om;
  private OzoneManagerRatisServer omRatisServer;

  private static final long LEADER_ELECTION_TIMEOUT = 500L;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    omId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    final String path = GenericTestUtils.getTempPath(omId);
    Path metaDirPath = Paths.get(path, "om-meta");
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.setTimeDuration(
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        LEADER_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);

    OMStorage omStore = new OMStorage(conf);
    omStore.setClusterId("testClusterId");
    omStore.setScmId("testScmId");
    // writes the version file properties
    omStore.initialize();

    cluster = MiniOzoneCluster.newHABuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOMServiceId(omServiceId).setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();

    om = cluster.getOzoneManager();
    omRatisServer = om.getOmRatisServer();

    Assert.assertEquals(LifeCycle.State.RUNNING, om.getOmRatisServerState());
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test configurating an OM service with three OM nodes.
   * Inspired by TestOzoneManagerConfiguration
   * @throws Exception
   */
  @Test
  public void testHAURLs1() throws Exception {
    // Inspired by TestOzoneManagerHA#setupBucket
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    ObjectStore objectStore =
        OzoneClientFactory.getRpcClient(omServiceId, conf).getObjectStore();
    objectStore.createVolume(volumeName);

    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    retVolumeinfo.createBucket(bucketName);
    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    /*
    // create a volume and a bucket to be used by OzoneFileSystem,
    // borrowed from TestOzoneFileSystem#init
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String rootPath = String.format("%s://%s.%s." + omServiceId,
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
     */
    String rootPath = String
        .format("%s://%s.%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucketName,
            volumeName, omServiceId);
    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem fs = FileSystem.get(conf);

    // Create random stuff
    org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path("/");
    org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(root, "dir1");
    org.apache.hadoop.fs.Path dir12 =
        new org.apache.hadoop.fs.Path(dir1, "dir12");
    org.apache.hadoop.fs.Path dir2 = new org.apache.hadoop.fs.Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);
/*
    FileStatus[] fileStatuses = fs.listStatus(root);
    for (FileStatus fileStatus : fileStatuses) {
      System.out.println(fileStatus);
    }
    System.out.println("!!! done creating stuff");
 */

    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    // Inspired by TestFsShell#testTracing
    clientConf.setQuietMode(false);
    // TODO: Do not hard code key
//    System.out.println("!!! Previous fs.o3fs.impl=" + clientConf.get("fs.o3fs.impl"));
    clientConf.set("fs.o3fs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem");
//    System.out.println("!!! Previous fs.defaultFS=" + clientConf.get("fs.defaultFS"));
    clientConf.set("fs.defaultFS", rootPath);
//    System.out.println("!!! New fs.defaultFS=" + clientConf.get("fs.defaultFS"));
//    System.out.println("!!! Previous ozone.om.address=" + clientConf.get("ozone.om.address"));

    // Pick the first OM's RPC address and assign it to ozone.om.address
    // for test case: ozone fs -ls o3fs://bucket.volume.om1/
    // TODO: Modularize this snippet
    String omNodesKey = "ozone.om.nodes." + omServiceId;
    String[] omNodes = clientConf.get(omNodesKey).split(",");
    assert(omNodes.length == 3);
    String omNode1 = omNodes[0];
    String omNodeAddressKey1 = "ozone.om.address." + omServiceId + "." + omNode1;
    String omNodeAddress1 = clientConf.get(omNodeAddressKey1);

    int omNodeAddressPort1 = OmUtils.getOmRpcPort(clientConf, omNodeAddressKey1);
    System.out.println(omNodeAddressPort1);

    clientConf.set("ozone.om.address", omNodeAddress1);

    // Compose fs command
    FsShell shell = new FsShell(clientConf);
    int res;
    try {
      // Test case 1: ozone fs -ls /
      // Expectation: Success.
      res = ToolRunner.run(shell, new String[] { "-ls", "/" });
      // Check return value
      Assert.assertEquals(res, 0);

      // Test case 2: ozone fs -ls o3fs:///
      // Expectation: Success. fs.defaultFS is a fully qualified path.
      res = ToolRunner.run(shell, new String[] { "-ls", "o3fs:///" });
      Assert.assertEquals(res, 0);

      // Test case 3: ozone fs -ls o3fs://bucket.volume/
      // Expectation: Fail. Must have service id or host name when HA is enabled
      String unqualifiedPath1 = String.format("%s://%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);
      try (GenericTestUtils.SystemErrCapturer capture =
          new GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell, new String[] { "-ls", unqualifiedPath1 });
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
      // Expectation: Success. The client would use the port number
      // we set in ozone.om.address.
      String qualifiedPath1 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          om.getOmRpcServerAddr().getHostName());
//      System.out.println("!!! omNodeAddress1 = " + omNodeAddress1);
//      System.out.println("!!! qualifiedPath1 = " + qualifiedPath1);
      res = ToolRunner.run(shell, new String[] { "-ls", qualifiedPath1 });
      // TODO: this test case will fail if the port is not a leader node
      // Why does a read-only operation require a leader node?
      Assert.assertEquals(res, 0);

      // Test case 5: ozone fs -ls o3fs://bucket.volume.om1:port/
      // Expectation: Success.
      String qualifiedPath2 = String.format("%s://%s.%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName, omNodeAddress1);
      res = ToolRunner.run(shell, new String[] { "-ls", qualifiedPath2 });
      Assert.assertEquals(res, 0);

      // Test case 6: ozone fs -ls o3fs://bucket.volume.id1/
      String qualifiedPath3 = String
          .format("%s://%s.%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucketName,
              volumeName, omServiceId);
      res = ToolRunner.run(shell, new String[] { "-ls", qualifiedPath3 });
      // Should succeed
      Assert.assertEquals(res, 0);

      // Test case 7: ozone fs -ls o3fs://bucket.volume.id1:port/
      // Expectation: Fail. Service ID does not use port information.
      // Fill in the port number with an OM's port (doesn't really matter)
      String unqualifiedPath2 = String.format("%s://%s.%s.%s:%d/",
          OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName,
          omServiceId, omNodeAddressPort1);
      try (GenericTestUtils.SystemErrCapturer capture =
          new GenericTestUtils.SystemErrCapturer()) {
        res = ToolRunner.run(shell, new String[] { "-ls", unqualifiedPath2 });
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

  @Test
  public void testHAURLs2() throws Exception {
    // Change fs.defaultFS to test other scenarios
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    // Inspired by TestFsShell#testTracing
    clientConf.setQuietMode(false);
    // TODO: Do not hard code key
    clientConf.set("fs.o3fs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem");
    clientConf.set("fs.defaultFS", "file:///");
    // Compose fs command
    FsShell shell = new FsShell(clientConf);
    try {
      // Test case: ozone fs -ls o3fs:///
      // Expectation: Fail. fs.defaultFS does not begin with o3fs://
      int res = ToolRunner.run(shell, new String[] { "-ls", "o3fs:///" });
      Assert.assertEquals(res, -1);
    } finally {
      shell.close();
    }
  }
}
