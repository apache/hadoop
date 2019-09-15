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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.web.ozShell.OzoneShell;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.fail;

/**
 * This class tests Ozone sh shell command.
 * Inspired by TestS3Shell
 */
public class TestOzoneShellHA {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShellHA.class);

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
  private static OzoneShell ozoneShell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  private static String omServiceId;
  private static String clusterId;
  private static String scmId;
  private static int numOfOMs;

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
        TestOzoneShellHA.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    s3Shell = new S3Shell();
    ozoneShell = new OzoneShell();

    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    conf.setQuietMode(false);
//    client = new RpcClient(conf, omServiceId);
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
    // TODO: Replace this with an address with service id
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

  private void execute(OzoneShell shell, String[] args) {
    LOG.info("Executing OzoneShell command with args {}", Arrays.asList(args));
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

    // Since there is no elegant way to pass Ozone config to the shell,
    // the idea is to use 'set' to place those OM HA configs.
    String[] argsWithHAConf = getHASetConfStrings(args);

    cmd.parseWithHandlers(new RunLast(), exceptionHandler, argsWithHAConf);
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, conf.get(key));
  }

  private String generateSetConfString(String key, String value) {
    return String.format("--set=%s=%s", key, value);
  }

  private String[] getHASetConfStrings(int numOfArgs) {
    assert(numOfArgs >= 0);
    String[] res = new String[1 + 1 + numOfOMs + numOfArgs];
    final int indexOmServiceIds = 0;
    final int indexOmNodes = 1;
    final int indexOmAddressStart = 2;

    res[indexOmServiceIds] = getSetConfStringFromConf(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = OmUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    String omNodesVal = conf.get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    // TODO: Use defined delimiter instead, if there is one
    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    assert(omNodesArr.length == numOfOMs);
    for (int i = 0; i < numOfOMs; i++) {
      res[indexOmAddressStart + i] =
          getSetConfStringFromConf(OmUtils.addKeySuffixes(
              OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodesArr[i]));
    }

    LOG.info(String.join("\n", res));
    return res;
  }

  private String[] getHASetConfStrings(String[] existingArgs) {
    // Get a String array populated with HA configs first
    String[] res = getHASetConfStrings(existingArgs.length);

    int indexCopyStart = res.length - existingArgs.length;
    // Then copy the existing args to the returned String array
    for (int i = 0; i < existingArgs.length; i++) {
      res[indexCopyStart + i] = existingArgs[i];
    }
    return res;
  }

  @Test
  public void testCreateVolume() throws IOException {
    /*
      ozone sh volume create /volume
      ozone sh bucket create /volume/bucket
      ozone sh volume create o3://om1/volume
      ozone sh volume create o3://om1:port/volume
      ozone sh bucket create o3://om1/volume/bucket
      ozone sh volume create o3://id1/volume
      ozone sh volume create o3://id1:port/volume
     */

    // ozone sh volume create o3://localhost:63620/volume
    String[] args = new String[] {"volume", "create",
        "o3://" + omServiceId + "/volume"};
    execute(ozoneShell, args);
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
