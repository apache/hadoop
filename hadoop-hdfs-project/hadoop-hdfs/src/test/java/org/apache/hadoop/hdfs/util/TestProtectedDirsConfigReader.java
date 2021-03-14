/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.NoSuchFileException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for ProtectedDirsConfigReader.
 */
public class TestProtectedDirsConfigReader {

  // Using /test/build/data/tmp directory to store temprory files.
  private final String pathTestDir =  GenericTestUtils
      .getTestDir().getAbsolutePath();

  private String configFile = pathTestDir + "/protected.dir.config";


  @After
  public void tearDown() throws Exception {
    // Delete test files after running tests
    new  File(configFile).delete();
  }

  /*
   * 1.Create protected.dirs.config file
   * 2.Write path names per line
   * 3.Write comments starting with #
   * 4.Close file
   * 5.Compare if number of paths reported by ProtectedDirConfigFileReader
   *   are equal to the number of paths written
   */
  @Test
  public void testProtectedDirConfigFileReader() throws Exception {

    FileWriter cfw = new FileWriter(configFile);

    cfw.write("#PROTECTED-DIRS-LIST\n");
    cfw.write("/dira1/dira2\n");
    cfw.write("/dirb1/dirb2/dirb3\n");
    cfw.write("/dirc1/dirc2/dirc3\n");
    cfw.write("#This-is-comment\n");
    cfw.write("/dird1/dird2 # /diri1/diri2\n");
    cfw.write("/dird1/dird2 /dire1/dire2\n");
    cfw.close();

    ProtectedDirsConfigReader hfp = new ProtectedDirsConfigReader(configFile);

    int dirsLen = hfp.getProtectedDirectories().size();

    assertEquals(5, dirsLen);
    assertTrue(hfp.getProtectedDirectories().contains("/dire1/dire2"));
    assertFalse(hfp.getProtectedDirectories().contains("/dirh1/dirh2"));

  }

  /*
   * Test creating a new ProtectedDirConfigFileReader with nonexistent files
   */
  @Test
  public void testCreateReaderWithNonexistentFile() throws Exception {
    try {
      new ProtectedDirsConfigReader(
          pathTestDir + "/doesnt-exist");
      Assert.fail("Should throw FileNotFoundException");
    } catch (NoSuchFileException ex) {
      // Exception as expected
    }
  }


  /*
   * Test for null file
   */
  @Test
  public void testProtectedDirConfigFileReaderWithNull() throws Exception {
    FileWriter cfw = new FileWriter(configFile);

    cfw.close();

    ProtectedDirsConfigReader hfp = new ProtectedDirsConfigReader(configFile);

    int dirsLen = hfp.getProtectedDirectories().size();

    // TestCase1: Check if lines beginning with # are ignored
    assertEquals(0, dirsLen);

    // TestCase2: Check if given path names are reported
    // by getProtectedProtectedDirs.
    assertFalse(hfp.getProtectedDirectories().contains("/dire1/dire2"));
  }

  /*
   * Check if only comments can be written to paths file
   */
  @Test
  public void testProtectedDirConfigFileReaderWithCommentsOnly()
      throws Exception {
    FileWriter cfw = new FileWriter(configFile);

    cfw.write("#PROTECTED-DIRS-LIST\n");
    cfw.write("#This-is-comment\n");

    cfw.close();

    ProtectedDirsConfigReader hfp = new ProtectedDirsConfigReader(configFile);

    int dirsLen = hfp.getProtectedDirectories().size();

    assertEquals(0, dirsLen);
    assertFalse(hfp.getProtectedDirectories().contains("/dire1/dire2"));

  }

  /*
   * Test if spaces are allowed in path names
   */
  @Test
  public void testProtectedDirConfigFileReaderWithSpaces() throws Exception {

    FileWriter cfw = new FileWriter(configFile);

    cfw.write("#PROTECTED-DIRS-LIST\n");
    cfw.write("   somepath /dirb1/dirb2/dirb3");
    cfw.write("   /dirc1/dirc2/dirc3 # /dird1/dird2");
    cfw.close();

    ProtectedDirsConfigReader hfp = new ProtectedDirsConfigReader(configFile);

    int dirsLen = hfp.getProtectedDirectories().size();

    assertEquals(3, dirsLen);
    assertTrue(hfp.getProtectedDirectories().contains("/dirc1/dirc2/dirc3"));
    assertFalse(hfp.getProtectedDirectories().contains("/dire1/dire2"));
    assertFalse(hfp.getProtectedDirectories().contains("/dird1/dird2"));

  }

  /*
   * Test if spaces , tabs and new lines are allowed
   */
  @Test
  public void testProtectedDirConfigFileReaderWithTabs() throws Exception {
    FileWriter cfw = new FileWriter(configFile);

    cfw.write("#PROTECTED-DIRS-LIST\n");
    cfw.write("     \n");
    cfw.write("   somepath \t  /dirb1/dirb2/dirb3 \n /dird1/dird2");
    cfw.write("   /dirc1/dirc2/dirc3 \t # /dire1/dire2");
    cfw.close();

    ProtectedDirsConfigReader hfp = new ProtectedDirsConfigReader(configFile);

    int dirsLen = hfp.getProtectedDirectories().size();

    assertEquals(4, dirsLen);
    assertTrue(hfp.getProtectedDirectories().contains("/dirb1/dirb2/dirb3"));
    assertFalse(hfp.getProtectedDirectories().contains("/dire1/dire2"));

  }
}
