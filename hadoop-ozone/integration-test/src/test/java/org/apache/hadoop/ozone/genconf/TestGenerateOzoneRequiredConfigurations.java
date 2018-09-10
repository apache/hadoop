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
import org.apache.hadoop.test.GenericTestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;


/**
 * Tests GenerateOzoneRequiredConfigurations.
 */
public class TestGenerateOzoneRequiredConfigurations {
  private static File outputBaseDir;
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
  }

  /**
   * Cleans up the output base directory.
   */
  @AfterClass
  public static void cleanup() throws IOException {
    FileUtils.deleteDirectory(outputBaseDir);
  }

  /**
   * Tests a valid path and generates ozone-site.xml by calling
   * {@code GenerateOzoneRequiredConfigurations#generateConfigurations}.
   *
   * @throws Exception
   */
  @Test
  public void testGenerateConfigurations() throws Exception {
    File tempPath = getRandomTempDir();
    String[] args = new String[]{"-output", tempPath.getAbsolutePath()};

    Assert.assertEquals("Path is valid",
        true, GenerateOzoneRequiredConfigurations.isValidPath(args[1]));

    Assert.assertEquals("Permission is valid",
        true, GenerateOzoneRequiredConfigurations.canWrite(args[1]));

    Assert.assertEquals("Config file generated",
        0, GenerateOzoneRequiredConfigurations.generateConfigurations(args[1]));
  }

  /**
   * Tests ozone-site.xml generation by calling
   * {@code GenerateOzoneRequiredConfigurations#main}.
   *
   * @throws Exception
   */
  @Test
  public void testGenerateConfigurationsThroughMainMethod() throws Exception {
    File tempPath = getRandomTempDir();
    String[] args = new String[]{"-output", tempPath.getAbsolutePath()};
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream oldStream = System.out;
    try (PrintStream ps = new PrintStream(outContent)) {
      System.setOut(ps);
      GenerateOzoneRequiredConfigurations.main(args);
      Assert.assertThat(outContent.toString(), CoreMatchers.containsString(
          "ozone-site.xml has been generated at"));
      System.setOut(oldStream);
    }
  }

  /**
   * Test to avoid generating ozone-site.xml when invalid permission.
   * @throws Exception
   */
  @Test
  public void generateConfigurationsFailure() throws Exception {
    File tempPath = getRandomTempDir();
    tempPath.setReadOnly();
    String[] args = new String[]{"-output", tempPath.getAbsolutePath()};
    GenerateOzoneRequiredConfigurations.main(args);

    Assert.assertEquals("Path is valid",
        true, GenerateOzoneRequiredConfigurations.isValidPath(args[1]));

    Assert.assertEquals("Invalid permission",
        false, GenerateOzoneRequiredConfigurations.canWrite(args[1]));

    Assert.assertEquals("Config file not generated",
        1, GenerateOzoneRequiredConfigurations.generateConfigurations(args[1]));
    tempPath.setWritable(true);
  }

  /**
   * Test to avoid generating ozone-site.xml when invalid permission.
   * @throws Exception
   */
  @Test
  public void generateConfigurationsFailureForInvalidPath() throws Exception {
    File tempPath = getRandomTempDir();
    tempPath.setReadOnly();
    String[] args = new String[]{"-output",
        tempPath.getAbsolutePath() + "/ozone-site.xml"};
    GenerateOzoneRequiredConfigurations.main(args);

    Assert.assertEquals("Path is invalid", false,
        GenerateOzoneRequiredConfigurations.isValidPath(args[1]));

    Assert.assertEquals("Config file not generated", 1,
        GenerateOzoneRequiredConfigurations.generateConfigurations(args[1]));
    tempPath.setWritable(true);
  }

  private File getRandomTempDir() throws IOException {
    File tempDir = new File(outputBaseDir,
        RandomStringUtils.randomAlphanumeric(5));
    FileUtils.forceMkdir(tempDir);
    return tempDir;
  }
}
