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

import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class TestJarFinder {

  @Test
  public void testJar() throws Exception {

    //picking a class that is for sure in a JAR in the classpath
    String jar = JarFinder.getJar(LogFactory.class);
    Assert.assertTrue(new File(jar).exists());
  }

  private static void delete(File file) throws IOException {
    if (file.getAbsolutePath().length() < 5) {
      throw new IllegalArgumentException(
        MessageFormat.format("Path [{0}] is too short, not deleting",
                             file.getAbsolutePath()));
    }
    if (file.exists()) {
      if (file.isDirectory()) {
        File[] children = file.listFiles();
        if (children != null) {
          for (File child : children) {
            delete(child);
          }
        }
      }
      if (!file.delete()) {
        throw new RuntimeException(
          MessageFormat.format("Could not delete path [{0}]",
                               file.getAbsolutePath()));
      }
    }
  }

  @Test
  public void testExpandedClasspath() throws Exception {
    //picking a class that is for sure in a directory in the classpath
    //in this case the JAR is created on the fly
    String jar = JarFinder.getJar(TestJarFinder.class);
    Assert.assertTrue(new File(jar).exists());
  }

  @Test
  public void testExistingManifest() throws Exception {
    File dir = new File(System.getProperty("test.build.dir", "target/test-dir"),
                        TestJarFinder.class.getName() + "-testExistingManifest");
    delete(dir);
    dir.mkdirs();

    File metaInfDir = new File(dir, "META-INF");
    metaInfDir.mkdirs();
    File manifestFile = new File(metaInfDir, "MANIFEST.MF");
    Manifest manifest = new Manifest();
    OutputStream os = new FileOutputStream(manifestFile);
    manifest.write(os);
    os.close();

    File propsFile = new File(dir, "props.properties");
    Writer writer = new FileWriter(propsFile);
    new Properties().store(writer, "");
    writer.close();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JarOutputStream zos = new JarOutputStream(baos);
    JarFinder.jarDir(dir, "", zos);
    JarInputStream jis =
      new JarInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Assert.assertNotNull(jis.getManifest());
    jis.close();
  }

  @Test
  public void testNoManifest() throws Exception {
    File dir = new File(System.getProperty("test.build.dir", "target/test-dir"),
                        TestJarFinder.class.getName() + "-testNoManifest");
    delete(dir);
    dir.mkdirs();
    File propsFile = new File(dir, "props.properties");
    Writer writer = new FileWriter(propsFile);
    new Properties().store(writer, "");
    writer.close();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JarOutputStream zos = new JarOutputStream(baos);
    JarFinder.jarDir(dir, "", zos);
    JarInputStream jis =
      new JarInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Assert.assertNotNull(jis.getManifest());
    jis.close();
  }
}
