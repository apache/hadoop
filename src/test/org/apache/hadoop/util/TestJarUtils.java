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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

import junit.framework.TestCase;

public class TestJarUtils
  extends TestCase {

  private File resourcesDir;
  private File jarUtilsDir;
  private File outJar1;
  private File outJar2;
  private String sep = File.separator;

  protected void setUp()
    throws Exception {
    
    resourcesDir = new File(System.getProperty("test.build.resources","."));
    outJar1 = new File(resourcesDir, "outjar1.jar");
    outJar2 = new File(resourcesDir, "outjar2.jar");
    jarUtilsDir = new File(resourcesDir, "jarutils");
  }

  protected void tearDown()
    throws Exception {
    outJar1.delete();
    outJar2.delete();
  }

  public void testGetJarPath() {

    File dir1 = new File(jarUtilsDir, "dir1");
    File file1 = new File(dir1, "file1");
    assertEquals(JarUtils.getJarPath(dir1, file1), "file1");

    File dir2 = new File(jarUtilsDir, "dir2");
    String file21Path = "dir2-1" + sep + "file2-1";
    File file21 = new File(dir2, file21Path);
    assertEquals(JarUtils.getJarPath(dir2, file21), file21Path);
  }

  public void testJar()
    throws Exception {

    JarUtils.jar(jarUtilsDir, outJar1);
    FileInputStream fis = new FileInputStream(outJar1);
    JarInputStream jis = new JarInputStream(fis);
    JarEntry entry = null;
    Set <String> resources = new HashSet <String>();
    while ((entry = jis.getNextJarEntry()) != null) {
      resources.add(entry.getName());
    }

    jis.close();
    fis.close();

    boolean good = true;
    good = resources.contains("dir1" + sep + "file1")
      && resources.contains("dir2" + sep + "file2")
      && resources.contains("dir2" + sep + "dir2-1" + sep + "file2-1")
      && resources.contains("dir3" + sep + "file3");
    assertTrue(good);
    assertEquals(resources.size(), 4);
  }

  public void testIsJarOrZip()
    throws Exception {

    File file1 = new File(jarUtilsDir, "dir1" + sep + "file1");
    JarUtils.jar(jarUtilsDir, outJar1);
    assertTrue(JarUtils.isJarOrZipFile(outJar1));
    assertFalse(JarUtils.isJarOrZipFile(file1));
  }

  public void testJarAll()
    throws Exception {

    JarUtils.jar(jarUtilsDir, outJar1);
    File file3 = new File(jarUtilsDir, "dir3" + sep + "file3");
    File dir1 = new File(jarUtilsDir, "dir1");
    File[] resources = new File[3];
    resources[0] = dir1;
    resources[1] = outJar1;
    resources[2] = file3;

    JarUtils.jarAll(resources, outJar2, true);

    FileInputStream fis = new FileInputStream(outJar2);
    JarInputStream jis = new JarInputStream(fis);
    JarEntry entry = null;
    Set <String> allres = new HashSet <String>();
    while ((entry = jis.getNextJarEntry()) != null) {
      allres.add(entry.getName());
    }

    jis.close();
    fis.close();

    boolean good = true;
    good = allres.contains("dir1" + sep + "file1")
      && allres.contains("dir2" + sep + "file2")
      && allres.contains("dir2" + sep + "dir2-1" + sep + "file2-1")
      && allres.contains("dir3" + sep + "file3") && allres.contains("file3")
      && allres.contains("file1");
    assertTrue(good);
    assertEquals(allres.size(), 6);

    JarUtils.jarAll(resources, outJar2, false);

    FileInputStream fis2 = new FileInputStream(outJar2);
    JarInputStream jis2 = new JarInputStream(fis2);
    JarEntry entry2 = null;
    Set <String> allres2 = new HashSet <String>();
    while ((entry2 = jis2.getNextJarEntry()) != null) {
      allres2.add(entry2.getName());
    }

    jis2.close();
    fis2.close();

    boolean good2 = true;
    good = allres2.contains("outjar1.jar") && allres2.contains("file3")
      && allres2.contains("file1");
    assertTrue(good2);
    assertEquals(allres2.size(), 3);
  }

  public void testCopyJarContents()
    throws Exception {

    JarUtils.jar(jarUtilsDir, outJar1);

    FileOutputStream fos = new FileOutputStream(outJar2);
    JarOutputStream jos = new JarOutputStream(fos);

    JarUtils.copyJarContents(outJar1, jos);

    jos.close();
    fos.close();

    jos.close();
    fos.close();

    FileInputStream fis = new FileInputStream(outJar2);
    JarInputStream jis = new JarInputStream(fis);
    JarEntry entry = null;
    Set <String> allres = new HashSet <String>();
    while ((entry = jis.getNextJarEntry()) != null) {
      allres.add(entry.getName());
    }

    jis.close();
    fis.close();

    boolean good = true;
    good = allres.contains("dir1" + sep + "file1")
      && allres.contains("dir2" + sep + "file2")
      && allres.contains("dir2" + sep + "dir2-1" + sep + "file2-1")
      && allres.contains("dir3" + sep + "file3");
    assertTrue(good);
    assertEquals(allres.size(), 4);
  }
}
