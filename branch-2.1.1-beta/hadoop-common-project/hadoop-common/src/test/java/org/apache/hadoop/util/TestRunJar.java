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

import junit.framework.TestCase;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.fs.FileUtil;

public class TestRunJar extends TestCase {
  private File TEST_ROOT_DIR;

  private static final String TEST_JAR_NAME="test-runjar.jar";

  @Override
  @Before
  protected void setUp()
      throws Exception {
    TEST_ROOT_DIR =
        new File(System.getProperty("test.build.data", "/tmp"), getClass()
            .getSimpleName());
    if (!TEST_ROOT_DIR.exists()) {
      TEST_ROOT_DIR.mkdirs();
    }

    makeTestJar();
  }

  @Override
  @After
  protected void tearDown() {
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
    jstream.putNextEntry(new ZipEntry("foobar.txt"));
    jstream.closeEntry();
    jstream.putNextEntry(new ZipEntry("foobaz.txt"));
    jstream.closeEntry();
    jstream.close();
  }

  /**
   * Test default unjarring behavior - unpack everything
   */
  @Test
  public void testUnJar() throws Exception {
    File unjarDir = new File(TEST_ROOT_DIR, "unjar-all");
    assertFalse("unjar dir shouldn't exist at test start",
                new File(unjarDir, "foobar.txt").exists());

    // Unjar everything
    RunJar.unJar(new File(TEST_ROOT_DIR, TEST_JAR_NAME),
                 unjarDir);
    assertTrue("foobar unpacked",
               new File(unjarDir, "foobar.txt").exists());
    assertTrue("foobaz unpacked",
               new File(unjarDir, "foobaz.txt").exists());

  }

  /**
   * Test unjarring a specific regex
   */
  public void testUnJarWithPattern() throws Exception {
    File unjarDir = new File(TEST_ROOT_DIR, "unjar-pattern");
    assertFalse("unjar dir shouldn't exist at test start",
                new File(unjarDir, "foobar.txt").exists());

    // Unjar only a regex
    RunJar.unJar(new File(TEST_ROOT_DIR, TEST_JAR_NAME),
                 unjarDir,
                 Pattern.compile(".*baz.*"));
    assertFalse("foobar not unpacked",
                new File(unjarDir, "foobar.txt").exists());
    assertTrue("foobaz unpacked",
               new File(unjarDir, "foobaz.txt").exists());

  }
}