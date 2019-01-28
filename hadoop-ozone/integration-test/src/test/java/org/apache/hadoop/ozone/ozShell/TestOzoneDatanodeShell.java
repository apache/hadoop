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
package org.apache.hadoop.ozone.ozShell;

import com.google.common.base.Strings;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.cli.MissingSubcommandException;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.junit.Assert.fail;

/**
 * This test class specified for testing Ozone datanode shell command.
 */
@RunWith(value = Parameterized.class)
public class TestOzoneDatanodeShell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneDatanodeShell.class);

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static File baseDir;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static HddsDatanodeService datanode = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][]{
        {RpcClient.class},
        {RestClient.class}};
    return Arrays.asList(params);
  }

  @Parameterized.Parameter
  @SuppressWarnings("visibilitymodifier")
  public Class clientProtocol;
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

    datanode = new HddsDatanodeService();

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    conf.setInt(OZONE_REPLICATION, ReplicationFactor.THREE.getValue());
    conf.setQuietMode(false);
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


  private void executeDatanode(HddsDatanodeService hdds, String[] args) {
    LOG.info("Executing datanode command with args {}", Arrays.asList(args));
    CommandLine cmd = hdds.getCmd();

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
   * Execute command, assert exception message and returns true if error
   * was thrown and contains the specified usage string.
   */
  private void executeDatanodeWithError(HddsDatanodeService hdds, String[] args,
      String expectedError, String usage) {
    if (Strings.isNullOrEmpty(expectedError)) {
      executeDatanode(hdds, args);
    } else {
      try {
        executeDatanode(hdds, args);
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
              ((MissingSubcommandException) exceptionToCheck)
                  .getUsage().contains(usage));
        }
      }
    }
  }

  @Test
  public void testDatanodeIncompleteCommand() {
    LOG.info("Running testDatanodeIncompleteCommand");
    String expectedError = "Incomplete command";
    String[] args = new String[]{}; //executing 'ozone datanode'

    executeDatanodeWithError(datanode, args, expectedError,
        "Usage: ozone datanode [-hV] [--verbose] [-D=<String=String>]...");

  }
}