/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.utils;

import org.apache.hadoop.yarn.submarine.FileUtilitiesForTests;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * This class is to test {@link ClassPathUtilities}.
 */
public class TestClassPathUtilities {

  private static final String CLASSPATH_KEY = "java.class.path";
  private FileUtilitiesForTests fileUtils = new FileUtilitiesForTests();
  private static String originalClasspath;

  @BeforeClass
  public static void setUpClass() {
    originalClasspath = System.getProperty(CLASSPATH_KEY);
  }

  @Before
  public void setUp() {
    fileUtils.setup();
  }

  @After
  public void teardown() throws IOException {
    fileUtils.teardown();
    System.setProperty(CLASSPATH_KEY, originalClasspath);
  }

  private static void addFileToClasspath(File file) {
    String newClasspath = originalClasspath + ":" + file.getAbsolutePath();
    System.setProperty(CLASSPATH_KEY, newClasspath);
  }

  @Test
  public void findFileNotInClasspath() {
    File resultFile = ClassPathUtilities.findFileOnClassPath("bla");
    assertNull(resultFile);
  }

  @Test
  public void findFileOnClasspath() throws Exception {
    File testFile = fileUtils.createFileInTempDir("testFile");

    addFileToClasspath(testFile);
    File resultFile = ClassPathUtilities.findFileOnClassPath("testFile");

    assertNotNull(resultFile);
    assertEquals(testFile.getAbsolutePath(), resultFile.getAbsolutePath());
  }

  @Test
  public void findDirectoryOnClasspath() throws Exception {
    File testDir = fileUtils.createDirInTempDir("testDir");
    File testFile = fileUtils.createFileInDir(testDir, "testFile");

    addFileToClasspath(testDir);
    File resultFile = ClassPathUtilities.findFileOnClassPath("testFile");

    assertNotNull(resultFile);
    assertEquals(testFile.getAbsolutePath(), resultFile.getAbsolutePath());
  }

}