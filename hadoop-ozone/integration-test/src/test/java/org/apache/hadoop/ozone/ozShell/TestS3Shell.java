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

import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.web.ozShell.s3.S3Shell;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.S3_BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.web.ozShell.s3.GetS3SecretHandler.OZONE_GETS3SECRET_ERROR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test class specified for testing Ozone s3Shell command.
 */
public class TestS3Shell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3Shell.class);

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
  private static S3Shell s3Shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  /**
   * Create a MiniOzoneCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    String path = GenericTestUtils.getTempPath(
        TestS3Shell.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    s3Shell = new S3Shell();

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    conf.setInt(OZONE_REPLICATION, ReplicationFactor.THREE.getValue());
    conf.setQuietMode(false);
    client = new RpcClient(conf);
    cluster.waitForClusterToBeReady();
  }

  /**
   * shutdown MiniOzoneCluster.
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
  public void testS3BucketMapping() throws IOException {
    String setOmAddress =
        "--set=" + OZONE_OM_ADDRESS_KEY + "=" + getOmAddress();

    String s3Bucket = "bucket1";
    String commandOutput;
    createS3Bucket("ozone", s3Bucket);

    // WHEN
    String[] args =
        new String[] {setOmAddress, "path", s3Bucket};
    execute(s3Shell, args);

    // THEN
    commandOutput = out.toString();
    String volumeName = client.getOzoneVolumeName(s3Bucket);
    assertTrue(commandOutput.contains("Volume name for S3Bucket is : " +
        volumeName));
    assertTrue(commandOutput.contains(OzoneConsts.OZONE_URI_SCHEME + "://" +
        s3Bucket + "." + volumeName));
    out.reset();

    // Trying to get map for an unknown bucket
    args = new String[] {setOmAddress, "path", "unknownbucket"};
    executeWithError(s3Shell, args, S3_BUCKET_NOT_FOUND);

    // No bucket name
    args = new String[] {setOmAddress, "path"};
    executeWithError(s3Shell, args, "Missing required parameter");

    // Invalid bucket name
    args = new String[] {setOmAddress, "path", "/asd/multipleslash"};
    executeWithError(s3Shell, args, S3_BUCKET_NOT_FOUND);
  }

  @Test
  public void testS3SecretUnsecuredCluster() throws Exception {
    String setOmAddress =
        "--set=" + OZONE_OM_ADDRESS_KEY + "=" + getOmAddress();

    String output;

    String[] args = new String[] {setOmAddress, "getsecret"};
    execute(s3Shell, args);
    // Get the first line of output
    output = out.toString().split("\n")[0];

    assertTrue(output.equals(OZONE_GETS3SECRET_ERROR));
  }

  private void createS3Bucket(String userName, String s3Bucket) {
    try {
      client.createS3Bucket("ozone", s3Bucket);
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("S3_BUCKET_ALREADY_EXISTS", ex);
    }
  }

  private void execute(S3Shell shell, String[] args) {
    LOG.info("Executing s3Shell command with args {}", Arrays.asList(args));
    CommandLine cmd = shell.getCmd();

    IExceptionHandler2<List<Object>> exceptionHandler =
        new IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(ParameterException ex,
                                                   String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(ExecutionException ex,
                                                       ParseResult parseRes) {
            throw ex;
          }
        };
    cmd.parseWithHandlers(new RunLast(),
        exceptionHandler, args);
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown.
   */
  private void executeWithError(S3Shell shell, String[] args,
                                OMException.ResultCodes code) {
    try {
      execute(shell, args);
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
  private void executeWithError(S3Shell shell, String[] args,
                                String expectedError) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(shell, args);
    } else {
      try {
        execute(shell, args);
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
                  "Error of s3Shell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
        }
      }
    }
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
}
