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


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * A test to rest the RunJar class.
 */
public class TestMRCJCRunJar {

  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString();

  private static final String TEST_JAR_NAME = "testjar.jar";
  private static final String CLASS_NAME = "Hello.class";

  @Test
  public void testRunjar() throws Throwable {
    File outFile = new File(TEST_ROOT_DIR, "out");
    // delete if output file already exists.
    if (outFile.exists()) {
      outFile.delete();
    }
    File makeTestJar = makeTestJar();

    String[] args = new String[3];
    args[0] = makeTestJar.getAbsolutePath();
    args[1] = "org.apache.hadoop.util.Hello";
    args[2] = outFile.toString();
    RunJar.main(args);
    Assert.assertTrue("RunJar failed", outFile.exists());
  }

  private File makeTestJar() throws IOException {
    File jarFile = new File(TEST_ROOT_DIR, TEST_JAR_NAME);
    JarOutputStream jstream = new JarOutputStream(new FileOutputStream(jarFile));
    InputStream entryInputStream = this.getClass().getResourceAsStream(
        CLASS_NAME);
    ZipEntry entry = new ZipEntry("org/apache/hadoop/util/" + CLASS_NAME);
    jstream.putNextEntry(entry);
    BufferedInputStream bufInputStream = new BufferedInputStream(
        entryInputStream, 2048);
    int count;
    byte[] data = new byte[2048];
    while ((count = bufInputStream.read(data, 0, 2048)) != -1) {
      jstream.write(data, 0, count);
    }
    jstream.closeEntry();
    jstream.close();

    return jarFile;
  }
}