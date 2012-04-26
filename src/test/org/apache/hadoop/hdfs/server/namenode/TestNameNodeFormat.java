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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.Permission;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestNameNodeFormat extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestNameNodeFormat.class);
  File hdfsDir;
  String baseDir;
  Configuration config;

  @Before
  public void setUp() throws IOException {
    System.setSecurityManager(new NoExitSecurityManager());

    baseDir = System.getProperty("test.build.data", "build/test/data");

    hdfsDir = new File(baseDir, "dfs/name");
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir)) {
      throw new IOException("Could not delete test directory '" + hdfsDir + "'");
    }
    LOG.info("hdfsdir is " + hdfsDir.getAbsolutePath());

    // set the defaults before every test to make sure we have a clean state
    StartupOption.FORMAT.setConfirmationNeeded(true);
    StartupOption.FORMAT.setInteractive(true);

    config = new Configuration();
    config.set("dfs.name.dir", hdfsDir.getPath());
  }

  @After
  public void tearDown() throws IOException {
    System.setSecurityManager(null); // or save and restore original
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir)) {
      throw new IOException("Could not tearDown test directory '" + hdfsDir
          + "'");
    }
  }

  /**
   * Test namenode format with -format option. Format should succeed.
   * 
   * @throws IOException
   */
  @Test
  public void testFormat() throws IOException {

    String[] argv = { "-format" };
    try {
      NameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      // catch the exit code and check the status
      assertEquals("Format should have succeeded", 0, e.status);
    }
    // check if the version file exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertTrue("Check version file exists", version.exists());
  }

  /**
   * Test namenode format with -format -force option when the name directory
   * exists. Format should succeed.
   * 
   * @throws IOException
   */
  @Test
  public void testFormatWithForce() throws IOException {

    // create the directory
    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    String[] argv = { "-format", "-force" };
    try {
      NameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      // catch the exit code and check the status
      assertEquals("Format should have succeeded", 0, e.status);
    }

    // check if the version file exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertTrue("Check version file exists", version.exists());
  }

  /**
   * Test namenode format with -format -nonInteractive when the name directory
   * exists. Format should be aborted.
   * 
   * @throws IOException
   */
  @Test
  public void testFormatWithNonInteractive() throws IOException {

    // create the directory
    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    String[] argv = { "-format", "-nonInteractive" };
    try {
      NameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      // catch the exit code and check the status
      assertEquals("Format should have been aborted with exit code 1", 1,
          e.status);
    }

    // check if the version file does not exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertFalse("Check version should not exist", version.exists());
  }

  /**
   * Test namenode format with -format -nonInteractive when name directory does
   * not exist. Format should succeed.
   * 
   * @throws IOException
   */
  @Test
  public void testFormatWithNonInteractiveNameDirDoesNotExit()
      throws IOException {

    String[] argv = { "-format", "-nonInteractive" };
    try {
      NameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      // catch the exit code and check the status
      assertEquals("Format should have succeeded", 0, e.status);
    }
    // check if the version file exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertTrue("Check version file exists", version.exists());
  }

  /**
   * Test namenode format with -format -nonInteractive -force when the name
   * directory exists. Format should succeed.
   * 
   * @throws IOException
   */
  @Test
  public void testFormatWithNonInteractiveAndForce() throws IOException {

    // create the directory
    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    String[] argv = { "-format", "-nonInteractive", "-force" };
    try {
      NameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      // catch the exit code and check the status
      assertEquals("Format should have succeeded", 0, e.status);
    }

    // check if the version file exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertTrue("Check version file exists", version.exists());
  }

  /**
   * Test namenode format with -format when the name directory exists and enter
   * N when prompted. Format should be aborted.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testFormatWithoutForceEnterN() throws IOException,
      InterruptedException {

    // create the directory
    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    // capture the input stream
    InputStream origIn = System.in;
    ByteArrayInputStream bins = new ByteArrayInputStream("N\n".getBytes());
    System.setIn(bins);
    String[] argv = { "-format" };
    try {
      NameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should not have succeeded", 1, e.status);
    }

    System.setIn(origIn);

    // check if the version file does not exists.
    File version = new File(hdfsDir, "current/VERSION");
    assertFalse("Check version should not exist", version.exists());
  }

  /**
   * Test namenode format with -format option when the name directory exists and
   * enter Y when prompted. Format should succeed.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testFormatWithoutForceEnterY() throws IOException,
      InterruptedException {

    // create the directory
    if (!hdfsDir.mkdirs()) {
      fail("Failed to create dir " + hdfsDir.getPath());
    }

    // capture the input stream
    InputStream origIn = System.in;
    ByteArrayInputStream bins = new ByteArrayInputStream("Y\n".getBytes());
    System.setIn(bins);
    String[] argv = { "-format" };
    try {
      NameNode.createNameNode(argv, config);
      fail("createNameNode() did not call System.exit()");
    } catch (ExitException e) {
      assertEquals("Format should have succeeded", 0, e.status);
    }

    System.setIn(origIn);

    // check if the version file does exist.
    File version = new File(hdfsDir, "current/VERSION");
    assertTrue("Check version file should exist", version.exists());
  }

  private static class ExitException extends SecurityException {
    private static final long serialVersionUID = 1L;
    public final int status;

    public ExitException(int status) {
      super("There is no escape!");
      this.status = status;
    }
  }

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }
}