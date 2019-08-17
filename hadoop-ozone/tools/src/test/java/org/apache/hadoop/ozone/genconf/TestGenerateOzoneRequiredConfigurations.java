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

package org.apache.hadoop.ozone.genconf;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.ParameterException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests GenerateOzoneRequiredConfigurations.
 */
public class TestGenerateOzoneRequiredConfigurations {
  private static File outputBaseDir;
  private static GenerateOzoneRequiredConfigurations genconfTool;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestGenerateOzoneRequiredConfigurations.class);
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  /**
   * Creates output directory which will be used by the test-cases.
   * If a test-case needs a separate directory, it has to create a random
   * directory inside {@code outputBaseDir}.
   *
   * @throws Exception In case of exception while creating output directory.
   */
  @BeforeClass
  public static void init() throws Exception {
    outputBaseDir = GenericTestUtils.getTestDir();
    FileUtils.forceMkdir(outputBaseDir);
    genconfTool = new GenerateOzoneRequiredConfigurations();
  }

  @Before
  public void setup() throws Exception {
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

  /**
   * Cleans up the output base directory.
   */
  @AfterClass
  public static void cleanup() throws IOException {
    FileUtils.deleteDirectory(outputBaseDir);
  }

  private void execute(String[] args, String msg) {
    List<String> arguments = new ArrayList(Arrays.asList(args));
    LOG.info("Executing shell command with args {}", arguments);
    CommandLine cmd = genconfTool.getCmd();

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
    cmd.parseWithHandlers(new CommandLine.RunLast(),
        exceptionHandler, args);
    Assert.assertTrue(out.toString().contains(msg));
  }

  private void executeWithException(String[] args, String msg) {
    List<String> arguments = new ArrayList(Arrays.asList(args));
    LOG.info("Executing shell command with args {}", arguments);
    CommandLine cmd = genconfTool.getCmd();

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
    try{
      cmd.parseWithHandlers(new CommandLine.RunLast(),
          exceptionHandler, args);
    }catch(Exception ex){
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
  }

  /**
   * Tests a valid path and generates ozone-site.xml by calling
   * {@code GenerateOzoneRequiredConfigurations#generateConfigurations}.
   * Further verifies that all properties have a default value.
   *
   * @throws Exception
   */
  @Test
  public void testGenerateConfigurations() throws Exception {
    File tempPath = getRandomTempDir();
    String[] args = new String[]{tempPath.getAbsolutePath()};
    execute(args, "ozone-site.xml has been generated at " +
        tempPath.getAbsolutePath());

    //Fetch file generated by above line
    URL url = new File(tempPath.getAbsolutePath() + "/ozone-site.xml")
        .toURI().toURL();
    OzoneConfiguration oc = new OzoneConfiguration();
    List<OzoneConfiguration.Property> allProperties =
        oc.readPropertyFromXml(url);

    //Asserts all properties have a non-empty value
    for (OzoneConfiguration.Property p : allProperties) {
      Assert.assertTrue(
          p.getValue() != null && p.getValue().length() > 0);
    }
  }

  /**
   * Generates ozone-site.xml at specified path.
   * Verify that it does not overwrite if file already exists in path.
   *
   * @throws Exception
   */
  @Test
  public void testDoesNotOverwrite() throws Exception {
    File tempPath = getRandomTempDir();
    String[] args = new String[]{tempPath.getAbsolutePath()};
    execute(args, "ozone-site.xml has been generated at " +
        tempPath.getAbsolutePath());

    //attempt overwrite
    execute(args, "ozone-site.xml already exists at " +
            tempPath.getAbsolutePath() + " and will not be overwritten");

  }

  /**
   * Test to avoid generating ozone-site.xml when insufficient permission.
   * @throws Exception
   */
  @Test
  public void genconfFailureByInsufficientPermissions() throws Exception {
    File tempPath = getRandomTempDir();
    tempPath.setReadOnly();
    String[] args = new String[]{tempPath.getAbsolutePath()};
    executeWithException(args, "Insufficient permission.");
  }

  /**
   * Test to avoid generating ozone-site.xml when invalid path.
   * @throws Exception
   */
  @Test
  public void genconfFailureByInvalidPath() throws Exception {
    File tempPath = getRandomTempDir();
    String[] args = new String[]{"invalid-path"};
    executeWithException(args, "Invalid directory path.");
  }

  /**
   * Test to avoid generating ozone-site.xml when path not specified.
   * @throws Exception
   */
  @Test
  public void genconfPathNotSpecified() throws Exception {
    File tempPath = getRandomTempDir();
    String[] args = new String[]{};
    executeWithException(args, "Missing required parameter: <path>");
  }

  /**
   * Test to check help message.
   * @throws Exception
   */
  @Test
  public void genconfHelp() throws Exception {
    File tempPath = getRandomTempDir();
    String[] args = new String[]{"--help"};
    execute(args, "Usage: ozone genconf [-hV] [--verbose]");
  }

  private File getRandomTempDir() throws IOException {
    File tempDir = new File(outputBaseDir,
        RandomStringUtils.randomAlphanumeric(5));
    FileUtils.forceMkdir(tempDir);
    return tempDir;
  }
}
