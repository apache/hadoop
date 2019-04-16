/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.ozShell;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.cli.MissingSubcommandException;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.tracing.StringCodec;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLRights;
import org.apache.hadoop.ozone.OzoneAcl.OzoneACLType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.web.ozShell.OzoneShell;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.JsonUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.base.Strings;
import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.slf4j.event.Level.TRACE;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

/**
 * This test class specified for testing Ozone shell command.
 */
public class TestOzoneShell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShell.class);

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static String url;
  private static File baseDir;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static ClientProtocol client = null;
  private static Shell shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  /**
   * Create a MiniDFSCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    String path = GenericTestUtils.getTempPath(
        TestOzoneShell.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    shell = new OzoneShell();

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    conf.setInt(OZONE_REPLICATION, ReplicationFactor.THREE.getValue());
    conf.setQuietMode(false);
    client = new RpcClient(conf);
    cluster.waitForClusterToBeReady();
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

    if (baseDir != null) {
      FileUtil.fullyDelete(baseDir, true);
    }
  }

  @Before
  public void setup() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
    url = "o3://" + getOmAddress();
  }

  @After
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  @Test
  public void testCreateVolume() throws Exception {
    LOG.info("Running testCreateVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    testCreateVolume(volumeName, "");
    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    testCreateVolume("/////" + volumeName, "");
    testCreateVolume("/////", "Volume name is required");
    testCreateVolume("/////vol/123",
        "Invalid volume name. Delimiters (/) not allowed in volume name");
  }

  private void testCreateVolume(String volumeName, String errorMsg)
      throws Exception {
    err.reset();
    String userName = "bilbo";
    String[] args = new String[] {"volume", "create", url + "/" + volumeName,
        "--user", userName, "--root"};

    if (Strings.isNullOrEmpty(errorMsg)) {
      execute(shell, args);

    } else {
      executeWithError(shell, args, errorMsg);
      return;
    }

    String truncatedVolumeName =
        volumeName.substring(volumeName.lastIndexOf('/') + 1);
    OzoneVolume volumeInfo = client.getVolumeDetails(truncatedVolumeName);
    assertEquals(truncatedVolumeName, volumeInfo.getName());
    assertEquals(userName, volumeInfo.getOwner());
  }

  private void execute(Shell ozoneShell, String[] args) {
    LOG.info("Executing shell command with args {}", Arrays.asList(args));
    CommandLine cmd = ozoneShell.getCmd();

    IExceptionHandler2<List<Object>> exceptionHandler =
        new IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(ParameterException ex,
              String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(ExecutionException ex,
              ParseResult parseResult) {
            throw ex;
          }
        };
    cmd.parseWithHandlers(new RunLast(),
        exceptionHandler, args);
  }

  /**
   * Test to create volume without specifying --user or -u.
   * @throws Exception
   */
  @Test
  public void testCreateVolumeWithoutUser() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String[] args = new String[] {"volume", "create", url + "/" + volumeName,
        "--root"};

    execute(shell, args);

    String truncatedVolumeName =
        volumeName.substring(volumeName.lastIndexOf('/') + 1);
    OzoneVolume volumeInfo = client.getVolumeDetails(truncatedVolumeName);
    assertEquals(truncatedVolumeName, volumeInfo.getName());
    assertEquals(UserGroupInformation.getCurrentUser().getUserName(),
        volumeInfo.getOwner());
  }

  @Test
  public void testDeleteVolume() throws Exception {
    LOG.info("Running testDeleteVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = client.getVolumeDetails(volumeName);
    assertNotNull(volume);

    String[] args = new String[] {"volume", "delete", url + "/" + volumeName};
    execute(shell, args);
    String output = out.toString();
    assertTrue(output.contains("Volume " + volumeName + " is deleted"));

    // verify if volume has been deleted
    try {
      client.getVolumeDetails(volumeName);
      fail("Get volume call should have thrown.");
    } catch (OMException e) {
      Assert.assertEquals(VOLUME_NOT_FOUND, e.getResult());
    }


    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    volume = client.getVolumeDetails(volumeName);
    assertNotNull(volume);

    //volumeName prefixed with /
    String volumeNameWithSlashPrefix = "/" + volumeName;
    args = new String[] {"volume", "delete",
        url + "/" + volumeNameWithSlashPrefix};
    execute(shell, args);
    output = out.toString();
    assertTrue(output.contains("Volume " + volumeName + " is deleted"));

    // verify if volume has been deleted
    try {
      client.getVolumeDetails(volumeName);
      fail("Get volume call should have thrown.");
    } catch (OMException e) {
      Assert.assertEquals(VOLUME_NOT_FOUND, e.getResult());
    }
  }

  @Test
  public void testInfoVolume() throws Exception {
    LOG.info("Running testInfoVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);

    //volumeName supplied as-is
    String[] args = new String[] {"volume", "info", url + "/" + volumeName};
    execute(shell, args);

    String output = out.toString();
    assertTrue(output.contains(volumeName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    //volumeName prefixed with /
    String volumeNameWithSlashPrefix = "/" + volumeName;
    args = new String[] {"volume", "info",
        url + "/" + volumeNameWithSlashPrefix};
    execute(shell, args);

    output = out.toString();
    assertTrue(output.contains(volumeName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // test infoVolume with invalid volume name
    args = new String[] {"volume", "info",
        url + "/" + volumeName + "/invalid-name"};
    executeWithError(shell, args, "Invalid volume name. " +
        "Delimiters (/) not allowed in volume name");

    // get info for non-exist volume
    args = new String[] {"volume", "info", url + "/invalid-volume"};
    executeWithError(shell, args, VOLUME_NOT_FOUND);
  }

  @Test
  public void testShellIncompleteCommand() throws Exception {
    LOG.info("Running testShellIncompleteCommand");
    String expectedError = "Incomplete command";
    String[] args = new String[] {}; //executing 'ozone sh'

    executeWithError(shell, args, expectedError,
        "Usage: ozone sh [-hV] [--verbose] [-D=<String=String>]..." +
            " [COMMAND]");

    args = new String[] {"volume"}; //executing 'ozone sh volume'
    executeWithError(shell, args, expectedError,
        "Usage: ozone sh volume [-hV] [COMMAND]");

    args = new String[] {"bucket"}; //executing 'ozone sh bucket'
    executeWithError(shell, args, expectedError,
        "Usage: ozone sh bucket [-hV] [COMMAND]");

    args = new String[] {"key"}; //executing 'ozone sh key'
    executeWithError(shell, args, expectedError,
        "Usage: ozone sh key [-hV] [COMMAND]");
  }

  @Test
  public void testUpdateVolume() throws Exception {
    LOG.info("Running testUpdateVolume");
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String userName = "bilbo";
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = client.getVolumeDetails(volumeName);
    assertEquals(userName, vol.getOwner());
    assertEquals(OzoneQuota.parseQuota("100TB").sizeInBytes(), vol.getQuota());

    String[] args = new String[] {"volume", "update", url + "/" + volumeName,
        "--quota", "500MB"};
    execute(shell, args);
    vol = client.getVolumeDetails(volumeName);
    assertEquals(userName, vol.getOwner());
    assertEquals(OzoneQuota.parseQuota("500MB").sizeInBytes(), vol.getQuota());

    String newUser = "new-user";
    args = new String[] {"volume", "update", url + "/" + volumeName,
        "--user", newUser};
    execute(shell, args);
    vol = client.getVolumeDetails(volumeName);
    assertEquals(newUser, vol.getOwner());

    //volume with / prefix
    String volumeWithPrefix = "/" + volumeName;
    String newUser2 = "new-user2";
    args = new String[] {"volume", "update", url + "/" + volumeWithPrefix,
        "--user", newUser2};
    execute(shell, args);
    vol = client.getVolumeDetails(volumeName);
    assertEquals(newUser2, vol.getOwner());

    // test error conditions
    args = new String[] {"volume", "update", url + "/invalid-volume",
        "--user", newUser};
    executeWithError(shell, args, ResultCodes.VOLUME_NOT_FOUND);

    err.reset();
    args = new String[] {"volume", "update", url + "/invalid-volume",
        "--quota", "500MB"};
    executeWithError(shell, args, ResultCodes.VOLUME_NOT_FOUND);
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown.
   */
  private void executeWithError(Shell ozoneShell, String[] args,
      OMException.ResultCodes code) {

    try {
      execute(ozoneShell, args);
      fail("Exception is expected from command execution " + Arrays
          .asList(args));
    } catch (Exception ex) {
      Assert.assertEquals(OMException.class, ex.getCause().getClass());
      Assert.assertEquals(code, ((OMException) ex.getCause()).getResult());
    }

  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown.
   */
  private void executeWithError(Shell ozoneShell, String[] args,
      String expectedError) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(ozoneShell, args);
    } else {
      try {
        execute(ozoneShell, args);
        fail("Exception is expected from command execution " + Arrays
            .asList(args));
      } catch (Exception ex) {
        if (!Strings.isNullOrEmpty(expectedError)) {
          Throwable exceptionToCheck = ex;
          if (exceptionToCheck.getCause() != null) {
            exceptionToCheck = exceptionToCheck.getCause();
          }
          Assert.assertTrue(
              String.format(
                  "Error of shell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
        }
      }
    }
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown.
   */
  private void executeWithError(Shell ozoneShell, String[] args,
      Class exception) {
    if (Objects.isNull(exception)) {
      execute(ozoneShell, args);
    } else {
      try {
        execute(ozoneShell, args);
        fail("Exception is expected from command execution " + Arrays
            .asList(args));
      } catch (Exception ex) {
        LOG.error("Exception: ", ex);
        assertTrue(ex.getCause().getClass().getCanonicalName()
            .equals(exception.getCanonicalName()));
      }
    }
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown and contains the specified usage string.
   */
  private void executeWithError(Shell ozoneShell, String[] args,
      String expectedError, String usage) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(ozoneShell, args);
    } else {
      try {
        execute(ozoneShell, args);
        fail("Exception is expected from command execution " + Arrays
            .asList(args));
      } catch (Exception ex) {
        if (!Strings.isNullOrEmpty(expectedError)) {
          Throwable exceptionToCheck = ex;
          if (exceptionToCheck.getCause() != null) {
            exceptionToCheck = exceptionToCheck.getCause();
          }
          Assert.assertTrue(
              String.format(
                  "Error of shell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
          Assert.assertTrue(
              exceptionToCheck instanceof MissingSubcommandException);
          Assert.assertTrue(
              ((MissingSubcommandException)exceptionToCheck)
                  .getUsage().contains(usage));
        }
      }
    }
  }

  @Test
  public void testListVolume() throws Exception {
    LOG.info("Running testListVolume");
    String protocol = "rpcclient";
    String commandOutput, commandError;
    List<VolumeInfo> volumes;
    final int volCount = 20;
    final String user1 = "test-user-a-" + protocol;
    final String user2 = "test-user-b-" + protocol;

    // Create 20 volumes, 10 for user1 and another 10 for user2.
    for (int x = 0; x < volCount; x++) {
      String volumeName;
      String userName;

      if (x % 2 == 0) {
        // create volume [test-vol0, test-vol2, ..., test-vol18] for user1
        userName = user1;
        volumeName = "test-vol-" + protocol + x;
      } else {
        // create volume [test-vol1, test-vol3, ..., test-vol19] for user2
        userName = user2;
        volumeName = "test-vol-" + protocol + x;
      }
      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setQuota("100TB")
          .build();
      client.createVolume(volumeName, volumeArgs);
      OzoneVolume vol = client.getVolumeDetails(volumeName);
      assertNotNull(vol);
    }

    String[] args = new String[] {"volume", "list", url + "/abcde", "--user",
        user1, "--length", "100"};
    executeWithError(shell, args, "Invalid URI");

    err.reset();
    // test -length option
    args = new String[] {"volume", "list", url + "/", "--user",
        user1, "--length", "100"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(10, volumes.size());
    for (VolumeInfo volume : volumes) {
      assertEquals(volume.getOwner().getName(), user1);
      assertTrue(volume.getCreatedOn().contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"volume", "list", url + "/", "--user",
        user1, "--length", "2"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(2, volumes.size());

    // test --prefix option
    out.reset();
    args =
        new String[] {"volume", "list", url + "/", "--user", user1, "--length",
            "100", "--prefix", "test-vol-" + protocol + "1"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(5, volumes.size());
    // return volume names should be [test-vol10, test-vol12, ..., test-vol18]
    for (int i = 0; i < volumes.size(); i++) {
      assertEquals(volumes.get(i).getVolumeName(),
          "test-vol-" + protocol + ((i + 5) * 2));
      assertEquals(volumes.get(i).getOwner().getName(), user1);
    }

    // test -start option
    out.reset();
    args =
        new String[] {"volume", "list", url + "/", "--user", user2, "--length",
            "100", "--start", "test-vol-" + protocol + "15"};
    execute(shell, args);
    commandOutput = out.toString();
    volumes = (List<VolumeInfo>) JsonUtils
        .toJsonList(commandOutput, VolumeInfo.class);

    assertEquals(2, volumes.size());

    assertEquals(volumes.get(0).getVolumeName(), "test-vol-" + protocol + "17");
    assertEquals(volumes.get(1).getVolumeName(), "test-vol-" + protocol + "19");
    assertEquals(volumes.get(0).getOwner().getName(), user2);
    assertEquals(volumes.get(1).getOwner().getName(), user2);

    // test error conditions
    err.reset();
    args = new String[] {"volume", "list", url + "/", "--user",
        user2, "--length", "-1"};
    executeWithError(shell, args, "the length should be a positive number");

    err.reset();
    args = new String[] {"volume", "list", url + "/", "--user",
        user2, "--length", "invalid-length"};
    executeWithError(shell, args, "For input string: \"invalid-length\"");
  }

  @Test
  public void testCreateBucket() throws Exception {
    LOG.info("Running testCreateBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String[] args = new String[] {"bucket", "create",
        url + "/" + vol.getName() + "/" + bucketName};

    execute(shell, args);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);
    assertEquals(vol.getName(),
        bucketInfo.getVolumeName());
    assertEquals(bucketName, bucketInfo.getName());

    // test create a bucket in a non-exist volume
    args = new String[] {"bucket", "create",
        url + "/invalid-volume/" + bucketName};
    executeWithError(shell, args, VOLUME_NOT_FOUND);

    // test createBucket with invalid bucket name
    args = new String[] {"bucket", "create",
        url + "/" + vol.getName() + "/" + bucketName + "/invalid-name"};
    executeWithError(shell, args,
        "Invalid bucket name. Delimiters (/) not allowed in bucket name");
  }

  @Test
  public void testDeleteBucket() throws Exception {
    LOG.info("Running testDeleteBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);
    assertNotNull(bucketInfo);

    String[] args = new String[] {"bucket", "delete",
        url + "/" + vol.getName() + "/" + bucketName};
    execute(shell, args);

    // verify if bucket has been deleted in volume
    try {
      vol.getBucket(bucketName);
      fail("Get bucket should have thrown.");
    } catch (OMException e) {
      Assert.assertEquals(BUCKET_NOT_FOUND, e.getResult());
    }

    // test delete bucket in a non-exist volume
    args = new String[] {"bucket", "delete",
        url + "/invalid-volume" + "/" + bucketName};
    executeWithError(shell, args, VOLUME_NOT_FOUND);

    err.reset();
    // test delete non-exist bucket
    args = new String[] {"bucket", "delete",
        url + "/" + vol.getName() + "/invalid-bucket"};
    executeWithError(shell, args, BUCKET_NOT_FOUND);
  }

  @Test
  public void testInfoBucket() throws Exception {
    LOG.info("Running testInfoBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);

    String[] args = new String[] {"bucket", "info",
        url + "/" + vol.getName() + "/" + bucketName};
    execute(shell, args);

    String output = out.toString();
    assertTrue(output.contains(bucketName));
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    // test infoBucket with invalid bucket name
    args = new String[] {"bucket", "info",
        url + "/" + vol.getName() + "/" + bucketName + "/invalid-name"};
    executeWithError(shell, args,
        "Invalid bucket name. Delimiters (/) not allowed in bucket name");

    // test get info from a non-exist bucket
    args = new String[] {"bucket", "info",
        url + "/" + vol.getName() + "/invalid-bucket" + bucketName};
    executeWithError(shell, args,
        ResultCodes.BUCKET_NOT_FOUND);
  }

  @Test
  public void testUpdateBucket() throws Exception {
    LOG.info("Running testUpdateBucket");
    OzoneVolume vol = creatVolume();
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    vol.createBucket(bucketName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    int aclSize = bucket.getAcls().size();

    String[] args = new String[] {"bucket", "update",
        url + "/" + vol.getName() + "/" + bucketName, "--addAcl",
        "user:frodo:rw,group:samwise:r"};
    execute(shell, args);
    String output = out.toString();
    assertTrue(output.contains("createdOn")
        && output.contains(OzoneConsts.OZONE_TIME_ZONE));

    bucket = vol.getBucket(bucketName);
    assertEquals(2 + aclSize, bucket.getAcls().size());

    OzoneAcl acl = bucket.getAcls().get(aclSize);
    assertTrue(acl.getName().equals("frodo")
        && acl.getType() == OzoneACLType.USER
        && acl.getRights()== OzoneACLRights.READ_WRITE);

    args = new String[] {"bucket", "update",
        url + "/" + vol.getName() + "/" + bucketName, "--removeAcl",
        "user:frodo:rw"};
    execute(shell, args);

    bucket = vol.getBucket(bucketName);
    acl = bucket.getAcls().get(aclSize);
    assertEquals(1 + aclSize, bucket.getAcls().size());
    assertTrue(acl.getName().equals("samwise")
        && acl.getType() == OzoneACLType.GROUP
        && acl.getRights()== OzoneACLRights.READ);

    // test update bucket for a non-exist bucket
    args = new String[] {"bucket", "update",
        url + "/" + vol.getName() + "/invalid-bucket", "--addAcl",
        "user:frodo:rw"};
    executeWithError(shell, args, BUCKET_NOT_FOUND);
  }

  @Test
  public void testListBucket() throws Exception {
    LOG.info("Running testListBucket");
    List<BucketInfo> buckets;
    String commandOutput;
    int bucketCount = 11;
    OzoneVolume vol = creatVolume();

    List<String> bucketNames = new ArrayList<>();
    // create bucket from test-bucket0 to test-bucket10
    for (int i = 0; i < bucketCount; i++) {
      String name = "test-bucket" + i;
      bucketNames.add(name);
      vol.createBucket(name);
      OzoneBucket bucket = vol.getBucket(name);
      assertNotNull(bucket);
    }

    // test listBucket with invalid volume name
    String[] args = new String[] {"bucket", "list",
        url + "/" + vol.getName() + "/invalid-name"};
    executeWithError(shell, args, "Invalid volume name. " +
        "Delimiters (/) not allowed in volume name");

    // test -length option
    args = new String[] {"bucket", "list",
        url + "/" + vol.getName(), "--length", "100"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(11, buckets.size());
    // sort bucket names since the return buckets isn't in created order
    Collections.sort(bucketNames);
    // return bucket names should be [test-bucket0, test-bucket1,
    // test-bucket10, test-bucket2, ,..., test-bucket9]
    for (int i = 0; i < buckets.size(); i++) {
      assertEquals(buckets.get(i).getBucketName(), bucketNames.get(i));
      assertEquals(buckets.get(i).getVolumeName(), vol.getName());
      assertTrue(buckets.get(i).getCreatedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "3"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(3, buckets.size());
    // return bucket names should be [test-bucket0,
    // test-bucket1, test-bucket10]
    assertEquals(buckets.get(0).getBucketName(), "test-bucket0");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket1");
    assertEquals(buckets.get(2).getBucketName(), "test-bucket10");

    // test --prefix option
    out.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "100", "--prefix", "test-bucket1"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(2, buckets.size());
    // return bucket names should be [test-bucket1, test-bucket10]
    assertEquals(buckets.get(0).getBucketName(), "test-bucket1");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket10");

    // test -start option
    out.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "100", "--start", "test-bucket7"};
    execute(shell, args);
    commandOutput = out.toString();
    buckets = (List<BucketInfo>) JsonUtils.toJsonList(commandOutput,
        BucketInfo.class);

    assertEquals(2, buckets.size());
    assertEquals(buckets.get(0).getBucketName(), "test-bucket8");
    assertEquals(buckets.get(1).getBucketName(), "test-bucket9");

    // test error conditions
    err.reset();
    args = new String[] {"bucket", "list", url + "/" + vol.getName(),
        "--length", "-1"};
    executeWithError(shell, args, "the length should be a positive number");
  }

  @Test
  public void testPutKey() throws Exception {
    LOG.info("Running testPutKey");
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    String[] args = new String[] {"key", "put",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName,
        createTmpFile()};
    execute(shell, args);

    OzoneKey keyInfo = bucket.getKey(keyName);
    assertEquals(keyName, keyInfo.getName());

    // test put key in a non-exist bucket
    args = new String[] {"key", "put",
        url + "/" + volumeName + "/invalid-bucket/" + keyName,
        createTmpFile()};
    executeWithError(shell, args, BUCKET_NOT_FOUND);
  }

  @Test
  public void testGetKey() throws Exception {
    GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer
        .captureLogs(StringCodec.LOG);
    GenericTestUtils.setLogLevel(StringCodec.LOG, TRACE);
    LOG.info("Running testGetKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();
    assertFalse("put key without malformed tracing",
        logs.getOutput().contains("MalformedTracerStateString"));
    logs.clearOutput();

    String tmpPath = baseDir.getAbsolutePath() + "/testfile-"
        + UUID.randomUUID().toString();
    String[] args = new String[] {"key", "get",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName,
        tmpPath};
    execute(shell, args);
    assertFalse("get key without malformed tracing",
        logs.getOutput().contains("MalformedTracerStateString"));
    logs.clearOutput();

    byte[] dataBytes = new byte[dataStr.length()];
    try (FileInputStream randFile = new FileInputStream(new File(tmpPath))) {
      randFile.read(dataBytes);
    }
    assertEquals(dataStr, DFSUtil.bytes2String(dataBytes));

    tmpPath = baseDir.getAbsolutePath() + File.separatorChar + keyName;
    args = new String[] {"key", "get",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName,
        baseDir.getAbsolutePath()};
    execute(shell, args);

    dataBytes = new byte[dataStr.length()];
    try (FileInputStream randFile = new FileInputStream(new File(tmpPath))) {
      randFile.read(dataBytes);
    }
    assertEquals(dataStr, DFSUtil.bytes2String(dataBytes));
  }

  @Test
  public void testDeleteKey() throws Exception {
    LOG.info("Running testDeleteKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

    OzoneKey keyInfo = bucket.getKey(keyName);
    assertEquals(keyName, keyInfo.getName());

    String[] args = new String[] {"key", "delete",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};
    execute(shell, args);

    // verify if key has been deleted in the bucket
    assertKeyNotExists(bucket, keyName);

    // test delete key in a non-exist bucket
    args = new String[] {"key", "delete",
        url + "/" + volumeName + "/invalid-bucket/" + keyName};
    executeWithError(shell, args, BUCKET_NOT_FOUND);

    err.reset();
    // test delete a non-exist key in bucket
    args = new String[] {"key", "delete",
        url + "/" + volumeName + "/" + bucketName + "/invalid-key"};
    executeWithError(shell, args, KEY_NOT_FOUND);
  }

  @Test
  public void testRenameKey() throws Exception {
    LOG.info("Running testRenameKey");
    OzoneBucket bucket = creatBucket();
    OzoneKey oldKey = createTestKey(bucket);

    String oldName = oldKey.getName();
    String newName = oldName + ".new";
    String[] args = new String[]{
        "key", "rename",
        String.format("%s/%s/%s",
            url, oldKey.getVolumeName(), oldKey.getBucketName()),
        oldName,
        newName
    };
    execute(shell, args);

    OzoneKey newKey = bucket.getKey(newName);
    assertEquals(oldKey.getCreationTime(), newKey.getCreationTime());
    assertEquals(oldKey.getDataSize(), newKey.getDataSize());
    assertKeyNotExists(bucket, oldName);
  }

  @Test
  public void testInfoKeyDetails() throws Exception {
    LOG.info("Running testInfoKey");
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(keyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();

    String[] args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/" + keyName};

    // verify the response output
    execute(shell, args);
    String output = out.toString();

    assertTrue(output.contains(keyName));
    assertTrue(
        output.contains("createdOn") && output.contains("modifiedOn") && output
            .contains(OzoneConsts.OZONE_TIME_ZONE));
    assertTrue(
        output.contains("containerID") && output.contains("localID") && output
            .contains("length") && output.contains("offset"));
    // reset stream
    out.reset();
    err.reset();

    // get the info of a non-exist key
    args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/invalid-key"};

    // verify the response output
    // get the non-exist key info should be failed
    executeWithError(shell, args, KEY_NOT_FOUND);
  }

  @Test
  public void testInfoDirKey() throws Exception {
    LOG.info("Running testInfoKey for Dir Key");
    String dirKeyName = "test/";
    String keyNameOnly = "test";
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    String dataStr = "test-data";
    OzoneOutputStream keyOutputStream =
        bucket.createKey(dirKeyName, dataStr.length());
    keyOutputStream.write(dataStr.getBytes());
    keyOutputStream.close();
    String[] args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/" + dirKeyName};
    // verify the response output
    execute(shell, args);
    String output = out.toString();
    assertTrue(output.contains(dirKeyName));
    assertTrue(output.contains("createdOn") &&
                output.contains("modifiedOn") &&
                output.contains(OzoneConsts.OZONE_TIME_ZONE));
    args = new String[] {"key", "info",
        url + "/" + volumeName + "/" + bucketName + "/" + keyNameOnly};
    executeWithError(shell, args, KEY_NOT_FOUND);
    out.reset();
    err.reset();
  }

  @Test
  public void testListKey() throws Exception {
    LOG.info("Running testListKey");
    String commandOutput;
    List<KeyInfo> keys;
    int keyCount = 11;
    OzoneBucket bucket = creatBucket();
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    String keyName;
    List<String> keyNames = new ArrayList<>();
    for (int i = 0; i < keyCount; i++) {
      keyName = "test-key" + i;
      keyNames.add(keyName);
      String dataStr = "test-data";
      OzoneOutputStream keyOutputStream =
          bucket.createKey(keyName, dataStr.length());
      keyOutputStream.write(dataStr.getBytes());
      keyOutputStream.close();
    }

    // test listKey with invalid bucket name
    String[] args = new String[] {"key", "list",
        url + "/" + volumeName + "/" + bucketName + "/invalid-name"};
    executeWithError(shell, args, "Invalid bucket name. " +
        "Delimiters (/) not allowed in bucket name");

    // test -length option
    args = new String[] {"key", "list",
        url + "/" + volumeName + "/" + bucketName, "--length", "100"};
    execute(shell, args);
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(11, keys.size());
    // sort key names since the return keys isn't in created order
    Collections.sort(keyNames);
    // return key names should be [test-key0, test-key1,
    // test-key10, test-key2, ,..., test-key9]
    for (int i = 0; i < keys.size(); i++) {
      assertEquals(keys.get(i).getKeyName(), keyNames.get(i));
      // verify the creation/modification time of key
      assertTrue(keys.get(i).getCreatedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
      assertTrue(keys.get(i).getModifiedOn()
          .contains(OzoneConsts.OZONE_TIME_ZONE));
    }

    out.reset();
    String msgText = "Listing first 3 entries of the result. " +
        "Use --length (-l) to override max returned keys.";
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "3"};
    execute(shell, args);
    commandOutput = out.toString();
    assertTrue("Expecting output to start with " + msgText,
        commandOutput.contains(msgText));
    commandOutput = commandOutput.replace(msgText, "");
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(3, keys.size());
    // return key names should be [test-key0, test-key1, test-key10]
    assertEquals(keys.get(0).getKeyName(), "test-key0");
    assertEquals(keys.get(1).getKeyName(), "test-key1");
    assertEquals(keys.get(2).getKeyName(), "test-key10");

    // test --prefix option
    out.reset();
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "100", "--prefix", "test-key1"};
    execute(shell, args);
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(2, keys.size());
    // return key names should be [test-key1, test-key10]
    assertEquals(keys.get(0).getKeyName(), "test-key1");
    assertEquals(keys.get(1).getKeyName(), "test-key10");

    // test -start option
    out.reset();
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "100", "--start", "test-key7"};
    execute(shell, args);
    commandOutput = out.toString();
    keys = (List<KeyInfo>) JsonUtils.toJsonList(commandOutput,
        KeyInfo.class);

    assertEquals(keys.get(0).getKeyName(), "test-key8");
    assertEquals(keys.get(1).getKeyName(), "test-key9");

    // test error conditions
    err.reset();
    args =
        new String[] {"key", "list", url + "/" + volumeName + "/" + bucketName,
            "--length", "-1"};
    executeWithError(shell, args, "the length should be a positive number");
  }

  private OzoneVolume creatVolume() throws OzoneException, IOException {
    String volumeName = RandomStringUtils.randomNumeric(5) + "volume";
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .build();
    try {
      client.createVolume(volumeName, volumeArgs);
    } catch (Exception ex) {
      Assert.assertEquals("PartialGroupNameException",
          ex.getCause().getClass().getSimpleName());
    }
    OzoneVolume volume = client.getVolumeDetails(volumeName);

    return volume;
  }

  private OzoneBucket creatBucket() throws OzoneException, IOException {
    OzoneVolume vol = creatVolume();
    String bucketName = RandomStringUtils.randomNumeric(5) + "bucket";
    vol.createBucket(bucketName);
    OzoneBucket bucketInfo = vol.getBucket(bucketName);

    return bucketInfo;
  }

  private OzoneKey createTestKey(OzoneBucket bucket) throws IOException {
    String key = "key" + RandomStringUtils.randomNumeric(5);
    String value = "value";

    OzoneOutputStream keyOutputStream =
        bucket.createKey(key, value.length());
    keyOutputStream.write(value.getBytes());
    keyOutputStream.close();

    return bucket.getKey(key);
  }

  @Test
  public void testTokenCommands() throws Exception {
    String omAdd = "--set=" + OZONE_OM_ADDRESS_KEY + "=" + getOmAddress();
    List<String[]> shellCommands = new ArrayList<>(4);
    // Case 1: Execution will fail when security is disabled.
    shellCommands.add(new String[]{omAdd, "token", "get"});
    shellCommands.add(new String[]{omAdd, "token", "renew"});
    shellCommands.add(new String[]{omAdd, "token", "cancel"});
    shellCommands.add(new String[]{omAdd, "token", "print"});
    shellCommands.forEach(cmd -> execute(cmd, "Error:Token operations " +
        "work only"));

    String security = "-D=" + OZONE_SECURITY_ENABLED_KEY + "=true";

    // Case 2: Execution of get token will fail when security is enabled but
    // OzoneManager is not setup correctly.
    execute(new String[]{omAdd, security,
        "token", "get"}, "Error: Get delegation token operation failed.");

    // Clear all commands.
    shellCommands.clear();

    // Case 3: Execution of renew/cancel/print token will fail as token file
    // doesn't exist.
    shellCommands.add(new String[]{omAdd, security, "token", "renew"});
    shellCommands.add(new String[]{omAdd, security, "token", "cancel"});
    shellCommands.add(new String[]{omAdd, security, "token", "print"});
    shellCommands.forEach(cmd -> execute(cmd, "token " +
        "operation failed as token file:"));

    // Create corrupt token file.
    File testPath = GenericTestUtils.getTestDir();
    Files.createDirectories(testPath.toPath());
    Path tokenFile = Paths.get(testPath.toString(), "token.txt");
    String question = RandomStringUtils.random(100);
    Files.write(tokenFile, question.getBytes());

    // Clear all commands.
    shellCommands.clear();
    String file = "-t=" + tokenFile.toString();

    // Case 4: Execution of renew/cancel/print token will fail if token file
    // is corrupt.
    shellCommands.add(new String[]{omAdd, security, "token", "renew", file});
    shellCommands.add(new String[]{omAdd, security, "token",
        "cancel", file});
    shellCommands.add(new String[]{omAdd, security, "token", "print", file});
    shellCommands.forEach(cmd -> executeWithError(shell, cmd,
        EOFException.class));
  }

  private void execute(String[] cmd, String msg) {
    // verify the response output
    execute(shell, cmd);
    String output = err.toString();
    assertTrue(output.contains(msg));
    // reset stream
    out.reset();
    err.reset();
  }

  /**
   * Create a temporary file used for putting key.
   * @return the created file's path string
   * @throws Exception
   */
  private String createTmpFile() throws Exception {
    // write a new file that used for putting key
    File tmpFile = new File(baseDir,
        "/testfile-" + UUID.randomUUID().toString());
    FileOutputStream randFile = new FileOutputStream(tmpFile);
    Random r = new Random();
    for (int x = 0; x < 10; x++) {
      char c = (char) (r.nextInt(26) + 'a');
      randFile.write(c);
    }
    randFile.close();

    return tmpFile.getAbsolutePath();
  }

  private String getOmAddress() {
    List<ServiceInfo> services;
    try {
      services = cluster.getOzoneManager().getServiceList();
    } catch (IOException e) {
      fail("Could not get service list from OM");
      return null;
    }

    return services.stream()
        .filter(a -> HddsProtos.NodeType.OM.equals(a.getNodeType()))
        .findFirst()
        .map(s -> s.getServiceAddress(ServicePort.Type.RPC))
        .orElseThrow(IllegalStateException::new);
  }

  private static void assertKeyNotExists(OzoneBucket bucket, String keyName)
      throws IOException {
    try {
      bucket.getKey(keyName);
      fail(String.format("Key %s should not exist, but it does", keyName));
    } catch (OMException e) {
      Assert.assertEquals(KEY_NOT_FOUND, e.getResult());
    }
  }

}
