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

package org.apache.hadoop.yarn.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.apache.hadoop.yarn.util.ApplicationClassLoader.constructUrlsFromClasspath;
import static org.apache.hadoop.yarn.util.ApplicationClassLoader.isSystemClass;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationClassLoader {
  
  private static File testDir = new File(System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")), "appclassloader");
  
  @Before
  public void setUp() {
    FileUtil.fullyDelete(testDir);
    testDir.mkdirs();
  }

  @Test
  public void testConstructUrlsFromClasspath() throws Exception {
    File file = new File(testDir, "file");
    assertTrue("Create file", file.createNewFile());

    File dir = new File(testDir, "dir");
    assertTrue("Make dir", dir.mkdir());

    File jarsDir = new File(testDir, "jarsdir");
    assertTrue("Make jarsDir", jarsDir.mkdir());
    File nonJarFile = new File(jarsDir, "nonjar");
    assertTrue("Create non-jar file", nonJarFile.createNewFile());
    File jarFile = new File(jarsDir, "a.jar");
    assertTrue("Create jar file", jarFile.createNewFile());

    File nofile = new File(testDir, "nofile");
    // don't create nofile

    StringBuilder cp = new StringBuilder();
    cp.append(file.getAbsolutePath()).append(File.pathSeparator)
      .append(dir.getAbsolutePath()).append(File.pathSeparator)
      .append(jarsDir.getAbsolutePath() + "/*").append(File.pathSeparator)
      .append(nofile.getAbsolutePath()).append(File.pathSeparator)
      .append(nofile.getAbsolutePath() + "/*").append(File.pathSeparator);
    
    URL[] urls = constructUrlsFromClasspath(cp.toString());
    
    assertEquals(3, urls.length);
    assertEquals(file.toURI().toURL(), urls[0]);
    assertEquals(dir.toURI().toURL(), urls[1]);
    assertEquals(jarFile.toURI().toURL(), urls[2]);
    // nofile should be ignored
  }
  
  @Test
  public void testIsSystemClass() {
    assertFalse(isSystemClass("org.example.Foo", null));
    assertTrue(isSystemClass("org.example.Foo", classes("org.example.Foo")));
    assertTrue(isSystemClass("/org.example.Foo", classes("org.example.Foo")));
    assertTrue(isSystemClass("org.example.Foo", classes("org.example.")));
    assertTrue(isSystemClass("net.example.Foo",
        classes("org.example.,net.example.")));
    assertFalse(isSystemClass("org.example.Foo",
        classes("-org.example.Foo,org.example.")));
    assertTrue(isSystemClass("org.example.Bar",
        classes("-org.example.Foo.,org.example.")));
  }
  
  private List<String> classes(String classes) {
    return Lists.newArrayList(Splitter.on(',').split(classes));
  }
  
  @Test
  public void testGetResource() throws IOException {
    URL testJar = makeTestJar().toURI().toURL();
    
    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader appClassloader = new ApplicationClassLoader(
        new URL[] { testJar }, currentClassLoader, null);

    assertNull("Resource should be null for current classloader",
        currentClassLoader.getResourceAsStream("resource.txt"));

    InputStream in = appClassloader.getResourceAsStream("resource.txt");
    assertNotNull("Resource should not be null for app classloader", in);
    assertEquals("hello", IOUtils.toString(in));
  }
  
  private File makeTestJar() throws IOException {
    File jarFile = new File(testDir, "test.jar");
    JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile));
    ZipEntry entry = new ZipEntry("resource.txt");
    out.putNextEntry(entry);
    out.write("hello".getBytes());
    out.closeEntry();
    out.close();
    return jarFile;
  }
  
}
