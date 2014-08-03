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
package org.apache.hadoop.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests covering the classpath command-line utility.
 */
public class TestClasspath {

  private static final Log LOG = LogFactory.getLog(TestClasspath.class);
  private static final File TEST_DIR = new File(
    System.getProperty("test.build.data", "/tmp"), "TestClasspath");
  private static final Charset UTF8 = Charset.forName("UTF-8");

  static {
    ExitUtil.disableSystemExit();
  }

  private PrintStream oldStdout, oldStderr;
  private ByteArrayOutputStream stdout, stderr;
  private PrintStream printStdout, printStderr;

  @Before
  public void setUp() {
    assertTrue(FileUtil.fullyDelete(TEST_DIR));
    assertTrue(TEST_DIR.mkdirs());
    oldStdout = System.out;
    oldStderr = System.err;

    stdout = new ByteArrayOutputStream();
    printStdout = new PrintStream(stdout);
    System.setOut(printStdout);

    stderr = new ByteArrayOutputStream();
    printStderr = new PrintStream(stderr);
    System.setErr(printStderr);
  }

  @After
  public void tearDown() {
    System.setOut(oldStdout);
    System.setErr(oldStderr);
    IOUtils.cleanup(LOG, printStdout, printStderr);
    assertTrue(FileUtil.fullyDelete(TEST_DIR));
  }

  @Test
  public void testGlob() {
    Classpath.main(new String[] { "--glob" });
    String strOut = new String(stdout.toByteArray(), UTF8);
    assertEquals(System.getProperty("java.class.path"), strOut.trim());
    assertTrue(stderr.toByteArray().length == 0);
  }

  @Test
  public void testJar() throws IOException {
    File file = new File(TEST_DIR, "classpath.jar");
    Classpath.main(new String[] { "--jar", file.getAbsolutePath() });
    assertTrue(stdout.toByteArray().length == 0);
    assertTrue(stderr.toByteArray().length == 0);
    assertTrue(file.exists());
    assertJar(file);
  }

  @Test
  public void testJarReplace() throws IOException {
    // Run the command twice with the same output jar file, and expect success.
    testJar();
    testJar();
  }

  @Test
  public void testJarFileMissing() throws IOException {
    try {
      Classpath.main(new String[] { "--jar" });
      fail("expected exit");
    } catch (ExitUtil.ExitException e) {
      assertTrue(stdout.toByteArray().length == 0);
      String strErr = new String(stderr.toByteArray(), UTF8);
      assertTrue(strErr.contains("requires path of jar"));
    }
  }

  @Test
  public void testHelp() {
    Classpath.main(new String[] { "--help" });
    String strOut = new String(stdout.toByteArray(), UTF8);
    assertTrue(strOut.contains("Prints the classpath"));
    assertTrue(stderr.toByteArray().length == 0);
  }

  @Test
  public void testHelpShort() {
    Classpath.main(new String[] { "-h" });
    String strOut = new String(stdout.toByteArray(), UTF8);
    assertTrue(strOut.contains("Prints the classpath"));
    assertTrue(stderr.toByteArray().length == 0);
  }

  @Test
  public void testUnrecognized() {
    try {
      Classpath.main(new String[] { "--notarealoption" });
      fail("expected exit");
    } catch (ExitUtil.ExitException e) {
      assertTrue(stdout.toByteArray().length == 0);
      String strErr = new String(stderr.toByteArray(), UTF8);
      assertTrue(strErr.contains("unrecognized option"));
    }
  }

  /**
   * Asserts that the specified file is a jar file with a manifest containing a
   * non-empty classpath attribute.
   *
   * @param file File to check
   * @throws IOException if there is an I/O error
   */
  private static void assertJar(File file) throws IOException {
    JarFile jarFile = null;
    try {
      jarFile = new JarFile(file);
      Manifest manifest = jarFile.getManifest();
      assertNotNull(manifest);
      Attributes mainAttributes = manifest.getMainAttributes();
      assertNotNull(mainAttributes);
      assertTrue(mainAttributes.containsKey(Attributes.Name.CLASS_PATH));
      String classPathAttr = mainAttributes.getValue(Attributes.Name.CLASS_PATH);
      assertNotNull(classPathAttr);
      assertFalse(classPathAttr.isEmpty());
    } finally {
      // It's too bad JarFile doesn't implement Closeable.
      if (jarFile != null) {
        try {
          jarFile.close();
        } catch (IOException e) {
          LOG.warn("exception closing jarFile: " + jarFile, e);
        }
      }
    }
  }
}
