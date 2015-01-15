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

package org.apache.hadoop.fs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.fs.HardLink.*;

/**
 * This testing is fairly lightweight.  Assumes HardLink routines will
 * only be called when permissions etc are okay; no negative testing is
 * provided.
 * 
 * These tests all use 
 * "src" as the source directory, 
 * "tgt_one" as the target directory for single-file hardlinking, and
 * "tgt_mult" as the target directory for multi-file hardlinking.
 * 
 * Contents of them are/will be:
 * dir:src: 
 *   files: x1, x2, x3
 * dir:tgt_one:
 *   files: x1 (linked to src/x1), y (linked to src/x2), 
 *          x3 (linked to src/x3), x11 (also linked to src/x1)
 * dir:tgt_mult:
 *   files: x1, x2, x3 (all linked to same name in src/)
 *   
 * NOTICE: This test class only tests the functionality of the OS
 * upon which the test is run! (although you're pretty safe with the
 * unix-like OS's, unless a typo sneaks in.)
 */
public class TestHardLink {
  
  public static final String TEST_ROOT_DIR = 
    System.getProperty("test.build.data", "build/test/data") + "/test";
  final static private File TEST_DIR = new File(TEST_ROOT_DIR, "hl");
  private static String DIR = "dir_";
  //define source and target directories
  private static File src = new File(TEST_DIR, DIR + "src");
  private static File tgt_mult = new File(TEST_DIR, DIR + "tgt_mult");
  private static File tgt_one = new File(TEST_DIR, DIR + "tgt_one");
  //define source files
  private static File x1 = new File(src, "x1");
  private static File x2 = new File(src, "x2");
  private static File x3 = new File(src, "x3");
  //define File objects for the target hardlinks
  private static File x1_one = new File(tgt_one, "x1");
  private static File y_one = new File(tgt_one, "y");
  private static File x3_one = new File(tgt_one, "x3");
  private static File x11_one = new File(tgt_one, "x11");
  private static File x1_mult = new File(tgt_mult, "x1");
  private static File x2_mult = new File(tgt_mult, "x2");
  private static File x3_mult = new File(tgt_mult, "x3");
  //content strings for file content testing
  private static String str1 = "11111";
  private static String str2 = "22222";
  private static String str3 = "33333";

  /**
   * Assure clean environment for start of testing
   * @throws IOException
   */
  @BeforeClass
  public static void setupClean() {
    //delete source and target directories if they exist
    FileUtil.fullyDelete(src);
    FileUtil.fullyDelete(tgt_one);
    FileUtil.fullyDelete(tgt_mult);
    //check that they are gone
    assertFalse(src.exists());
    assertFalse(tgt_one.exists());
    assertFalse(tgt_mult.exists());
  }
  
  /**
   * Initialize clean environment for start of each test
   */
  @Before
  public void setupDirs() throws IOException {
    //check that we start out with empty top-level test data directory
    assertFalse(src.exists());
    assertFalse(tgt_one.exists());
    assertFalse(tgt_mult.exists());
    //make the source and target directories
    src.mkdirs();
    tgt_one.mkdirs();
    tgt_mult.mkdirs();
    
    //create the source files in src, with unique contents per file
    makeNonEmptyFile(x1, str1);
    makeNonEmptyFile(x2, str2);
    makeNonEmptyFile(x3, str3);
    //validate
    validateSetup();
  }
  
  /**
   * validate that {@link setupDirs()} produced the expected result
   */
  private void validateSetup() throws IOException {
    //check existence of source directory and files
    assertTrue(src.exists());
    assertEquals(3, src.list().length);
    assertTrue(x1.exists());
    assertTrue(x2.exists());
    assertTrue(x3.exists());
    //check contents of source files
    assertTrue(fetchFileContents(x1).equals(str1));
    assertTrue(fetchFileContents(x2).equals(str2));
    assertTrue(fetchFileContents(x3).equals(str3));
    //check target directories exist and are empty
    assertTrue(tgt_one.exists());
    assertTrue(tgt_mult.exists());
    assertEquals(0, tgt_one.list().length);
    assertEquals(0, tgt_mult.list().length);    
  }
  
  /**
   * validate that single-file link operations produced the expected results
   */
  private void validateTgtOne() throws IOException {
    //check that target directory tgt_one ended up with expected four files
    assertTrue(tgt_one.exists());
    assertEquals(4, tgt_one.list().length);
    assertTrue(x1_one.exists());
    assertTrue(x11_one.exists());
    assertTrue(y_one.exists());
    assertTrue(x3_one.exists());
    //confirm the contents of those four files reflects the known contents
    //of the files they were hardlinked from.
    assertTrue(fetchFileContents(x1_one).equals(str1));
    assertTrue(fetchFileContents(x11_one).equals(str1));
    assertTrue(fetchFileContents(y_one).equals(str2));
    assertTrue(fetchFileContents(x3_one).equals(str3));
  }
  
  /**
   * validate that multi-file link operations produced the expected results
   */
  private void validateTgtMult() throws IOException {
    //check that target directory tgt_mult ended up with expected three files
    assertTrue(tgt_mult.exists());
    assertEquals(3, tgt_mult.list().length);
    assertTrue(x1_mult.exists());
    assertTrue(x2_mult.exists());
    assertTrue(x3_mult.exists());
    //confirm the contents of those three files reflects the known contents
    //of the files they were hardlinked from.
    assertTrue(fetchFileContents(x1_mult).equals(str1));
    assertTrue(fetchFileContents(x2_mult).equals(str2));
    assertTrue(fetchFileContents(x3_mult).equals(str3));
  }

  @After
  public void tearDown() throws IOException {
    setupClean();
  }

  private void makeNonEmptyFile(File file, String contents) 
  throws IOException {
    FileWriter fw = new FileWriter(file);
    fw.write(contents);
    fw.close();
  }
  
  private void appendToFile(File file, String contents) 
  throws IOException {
    FileWriter fw = new FileWriter(file, true);
    fw.write(contents);
    fw.close();
  }
  
  private String fetchFileContents(File file) 
  throws IOException {
    char[] buf = new char[20];
    FileReader fr = new FileReader(file);
    int cnt = fr.read(buf); 
    fr.close();
    char[] result = Arrays.copyOf(buf, cnt);
    return new String(result);
  }
  
  /**
   * Sanity check the simplest case of HardLink.getLinkCount()
   * to make sure we get back "1" for ordinary single-linked files.
   * Tests with multiply-linked files are in later test cases.
   */
  @Test
  public void testGetLinkCount() throws IOException {
    //at beginning of world, check that source files have link count "1"
    //since they haven't been hardlinked yet
    assertEquals(1, getLinkCount(x1));
    assertEquals(1, getLinkCount(x2));
    assertEquals(1, getLinkCount(x3));
  }

  /**
   * Test the single-file method HardLink.createHardLink().
   * Also tests getLinkCount() with values greater than one.
   */
  @Test
  public void testCreateHardLink() throws IOException {
    //hardlink a single file and confirm expected result
    createHardLink(x1, x1_one);
    assertTrue(x1_one.exists());
    assertEquals(2, getLinkCount(x1));     //x1 and x1_one are linked now 
    assertEquals(2, getLinkCount(x1_one)); //so they both have count "2"
    //confirm that x2, which we didn't change, still shows count "1"
    assertEquals(1, getLinkCount(x2));
    
    //now do a few more
    createHardLink(x2, y_one);
    createHardLink(x3, x3_one);
    assertEquals(2, getLinkCount(x2)); 
    assertEquals(2, getLinkCount(x3));

    //create another link to a file that already has count 2
    createHardLink(x1, x11_one);
    assertEquals(3, getLinkCount(x1));      //x1, x1_one, and x11_one
    assertEquals(3, getLinkCount(x1_one));  //are all linked, so they
    assertEquals(3, getLinkCount(x11_one)); //should all have count "3"
    
    //validate by contents
    validateTgtOne();
    
    //validate that change of content is reflected in the other linked files
    appendToFile(x1_one, str3);
    assertTrue(fetchFileContents(x1_one).equals(str1 + str3));
    assertTrue(fetchFileContents(x11_one).equals(str1 + str3));
    assertTrue(fetchFileContents(x1).equals(str1 + str3));
  }
  
  /*
   * Test the multi-file method HardLink.createHardLinkMult(),
   * multiple files within a directory into one target directory
   */
  @Test
  public void testCreateHardLinkMult() throws IOException {
    //hardlink a whole list of three files at once
    String[] fileNames = src.list();
    createHardLinkMult(src, fileNames, tgt_mult);
    
    //validate by link count - each file has been linked once,
    //so each count is "2"
    assertEquals(2, getLinkCount(x1));
    assertEquals(2, getLinkCount(x2));
    assertEquals(2, getLinkCount(x3));
    assertEquals(2, getLinkCount(x1_mult));
    assertEquals(2, getLinkCount(x2_mult));
    assertEquals(2, getLinkCount(x3_mult));

    //validate by contents
    validateTgtMult();
    
    //validate that change of content is reflected in the other linked files
    appendToFile(x1_mult, str3);
    assertTrue(fetchFileContents(x1_mult).equals(str1 + str3));
    assertTrue(fetchFileContents(x1).equals(str1 + str3));
  }

  /**
   * Test createHardLinkMult() with empty list of files.
   * We use an extended version of the method call, that
   * returns the number of System exec calls made, which should
   * be zero in this case.
   */
  @Test
  public void testCreateHardLinkMultEmptyList() throws IOException {
    String[] emptyList = {};
    
    //test the case of empty file list
    createHardLinkMult(src, emptyList, tgt_mult);
    //check nothing changed in the directory tree
    validateSetup();
  }

  /*
   * Assume that this test won't usually be run on a Windows box.
   * This test case allows testing of the correct syntax of the Windows
   * commands, even though they don't actually get executed on a non-Win box.
   * The basic idea is to have enough here that substantive changes will
   * fail and the author will fix and add to this test as appropriate.
   * 
   * Depends on the HardLinkCGWin class and member fields being accessible
   * from this test method.
   */
  @Test
  public void testWindowsSyntax() {
    class win extends HardLinkCGWin {}

    //basic checks on array lengths
    assertEquals(4, win.getLinkCountCommand.length);

    //make sure "%f" was not munged
    assertEquals(2, ("%f").length()); 
    //make sure "\\%f" was munged correctly
    assertEquals(3, ("\\%f").length()); 
    assertTrue(win.getLinkCountCommand[1].equals("hardlink"));
    //make sure "-c%h" was not munged
    assertEquals(4, ("-c%h").length()); 
  }

}
