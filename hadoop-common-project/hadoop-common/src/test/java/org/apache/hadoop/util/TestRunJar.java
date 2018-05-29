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

import static org.apache.hadoop.util.RunJar.MATCH_ANY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRunJar {
  private static final String FOOBAR_TXT = "foobar.txt";
  private static final String FOOBAZ_TXT = "foobaz.txt";
  private static final int BUFF_SIZE = 2048;
  private File TEST_ROOT_DIR;

  private static final String TEST_JAR_NAME="test-runjar.jar";
  private static final String TEST_JAR_2_NAME = "test-runjar2.jar";
  private static final long MOCKED_NOW = 1_460_389_972_000L;
  private static final long MOCKED_NOW_PLUS_TWO_SEC = MOCKED_NOW + 2_000;

  @Before
  public void setUp() throws Exception {
    TEST_ROOT_DIR = GenericTestUtils.getTestDir(getClass().getSimpleName());
    if (!TEST_ROOT_DIR.exists()) {
      TEST_ROOT_DIR.mkdirs();
    }

    makeTestJar();
  }

  @After
  public void tearDown() {
    FileUtil.fullyDelete(TEST_ROOT_DIR);
  }

  /**
   * Construct a jar with two files in it in our
   * test dir.
   */
  private void makeTestJar() throws IOException {
    File jarFile = new File(TEST_ROOT_DIR, TEST_JAR_NAME);
    JarOutputStream jstream =
        new JarOutputStream(new FileOutputStream(jarFile));
    ZipEntry zipEntry1 = new ZipEntry(FOOBAR_TXT);
    zipEntry1.setTime(MOCKED_NOW);
    jstream.putNextEntry(zipEntry1);
    jstream.closeEntry();
    ZipEntry zipEntry2 = new ZipEntry(FOOBAZ_TXT);
    zipEntry2.setTime(MOCKED_NOW_PLUS_TWO_SEC);
    jstream.putNextEntry(zipEntry2);
    jstream.closeEntry();
    jstream.close();
  }

  /**
   * Test default unjarring behavior - unpack everything
   */
  @Test
  public void testUnJar() throws Exception {
    File unjarDir = getUnjarDir("unjar-all");

    // Unjar everything
    RunJar.unJar(new File(TEST_ROOT_DIR, TEST_JAR_NAME),
                 unjarDir);
    assertTrue("foobar unpacked",
               new File(unjarDir, TestRunJar.FOOBAR_TXT).exists());
    assertTrue("foobaz unpacked",
               new File(unjarDir, FOOBAZ_TXT).exists());
  }

  /**
   * Test unjarring a specific regex
   */
  @Test
  public void testUnJarWithPattern() throws Exception {
    File unjarDir = getUnjarDir("unjar-pattern");

    // Unjar only a regex
    RunJar.unJar(new File(TEST_ROOT_DIR, TEST_JAR_NAME),
                 unjarDir,
                 Pattern.compile(".*baz.*"));
    assertFalse("foobar not unpacked",
                new File(unjarDir, TestRunJar.FOOBAR_TXT).exists());
    assertTrue("foobaz unpacked",
               new File(unjarDir, FOOBAZ_TXT).exists());
  }

  private File generateBigJar(File dir) throws Exception {
    File file = new File(dir, "job.jar");
    try(JarOutputStream stream = new JarOutputStream(
        new FileOutputStream(file))) {
      Random r = new Random(100);
      for (int i = 0; i < 10; ++i) {
        JarEntry entry = new JarEntry(
            ((i % 2 == 0) ? "dir/" : "") + "f" + Integer.toString(i));
        stream.putNextEntry(entry);
        for (int j=0; j < 756; ++j) {
          stream.write(r.nextInt() & 0xFF);
        }
        stream.closeEntry();
      }
      stream.close();
    }
    return file;
  }

  /**
   * Test unjarring a big file. This checks appending the remainder of the file
   * to the tee output stream in RunJar.unJarAndSave.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testBigJar() throws Exception {
    Random r = new Random(System.currentTimeMillis());
    File dir = new File(TEST_ROOT_DIR, Long.toHexString(r.nextLong()));
    Assert.assertTrue(dir.mkdirs());
    File input = generateBigJar(dir);
    File output = new File(dir, "job2.jar");
    try {
      try (InputStream is = new FileInputStream(input)) {
        RunJar.unJarAndSave(is, dir, "job2.jar", Pattern.compile(".*"));
      }
      Assert.assertEquals(input.length(), output.length());
      for (int i = 0; i < 10; ++i) {
        File subdir = new File(dir, ((i % 2 == 0) ? "dir/" : ""));
        File f = new File(subdir, "f" + Integer.toString(i));
        Assert.assertEquals(756, f.length());
      }
    } finally {
      // Clean up
      FileSystem fs = LocalFileSystem.getLocal(new Configuration());
      fs.delete(new Path(dir.getAbsolutePath()), true);
    }
  }

  @Test
  public void testUnJarDoesNotLooseLastModify() throws Exception {
    File unjarDir = getUnjarDir("unjar-lastmod");

    // Unjar everything
    RunJar.unJar(new File(TEST_ROOT_DIR, TEST_JAR_NAME),
            unjarDir);

    String failureMessage = "Last modify time was lost during unJar";
    assertEquals(failureMessage, MOCKED_NOW, new File(unjarDir, TestRunJar.FOOBAR_TXT).lastModified());
    assertEquals(failureMessage, MOCKED_NOW_PLUS_TWO_SEC, new File(unjarDir, FOOBAZ_TXT).lastModified());
  }

  private File getUnjarDir(String dirName) {
    File unjarDir = new File(TEST_ROOT_DIR, dirName);
    assertFalse("unjar dir shouldn't exist at test start",
                new File(unjarDir, TestRunJar.FOOBAR_TXT).exists());
    return unjarDir;
  }

  /**
   * Tests the client classloader to verify the main class and its dependent
   * class are loaded correctly by the application classloader, and others are
   * loaded by the system classloader.
   */
  @Test
  public void testClientClassLoader() throws Throwable {
    RunJar runJar = spy(new RunJar());
    // enable the client classloader
    when(runJar.useClientClassLoader()).thenReturn(true);
    // set the system classes and blacklist the test main class and the test
    // third class so they can be loaded by the application classloader
    String mainCls = ClassLoaderCheckMain.class.getName();
    String thirdCls = ClassLoaderCheckThird.class.getName();
    String systemClasses = "-" + mainCls + "," +
        "-" + thirdCls + "," +
        ApplicationClassLoader.SYSTEM_CLASSES_DEFAULT;
    when(runJar.getSystemClasses()).thenReturn(systemClasses);

    // create the test jar
    File testJar = JarFinder.makeClassLoaderTestJar(this.getClass(),
        TEST_ROOT_DIR, TEST_JAR_2_NAME, BUFF_SIZE, mainCls, thirdCls);
    // form the args
    String[] args = new String[3];
    args[0] = testJar.getAbsolutePath();
    args[1] = mainCls;

    // run RunJar
    runJar.run(args);
    // it should not throw an exception
  }

  @Test
  public void testUnJar2() throws IOException {
    // make a simple zip
    File jarFile = new File(TEST_ROOT_DIR, TEST_JAR_NAME);
    JarOutputStream jstream =
        new JarOutputStream(new FileOutputStream(jarFile));
    JarEntry je = new JarEntry("META-INF/MANIFEST.MF");
    byte[] data = "Manifest-Version: 1.0\nCreated-By: 1.8.0_1 (Manual)"
        .getBytes(StandardCharsets.UTF_8);
    je.setSize(data.length);
    jstream.putNextEntry(je);
    jstream.write(data);
    jstream.closeEntry();
    je = new JarEntry("../outside.path");
    data = "any data here".getBytes(StandardCharsets.UTF_8);
    je.setSize(data.length);
    jstream.putNextEntry(je);
    jstream.write(data);
    jstream.closeEntry();
    jstream.close();

    File unjarDir = getUnjarDir("unjar-path");

    // Unjar everything
    try {
      RunJar.unJar(jarFile, unjarDir, MATCH_ANY);
      fail("unJar should throw IOException.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "would create file outside of", e);
    }
    try {
      RunJar.unJar(new FileInputStream(jarFile), unjarDir, MATCH_ANY);
      fail("unJar should throw IOException.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "would create file outside of", e);
    }
  }
}
