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
import java.io.FileNotFoundException;
import java.io.FileWriter;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.HostsFileReader.HostDetails;
import org.junit.*;

import static org.junit.Assert.*;

/*
 * Test for HostsFileReader.java
 *
 */
public class TestHostsFileReader {

  // Using /test/build/data/tmp directory to store temprory files
  final String HOSTS_TEST_DIR = GenericTestUtils.getTestDir().getAbsolutePath();
  File EXCLUDES_FILE = new File(HOSTS_TEST_DIR, "dfs.exclude");
  File INCLUDES_FILE = new File(HOSTS_TEST_DIR, "dfs.include");
  String excludesFile = HOSTS_TEST_DIR + "/dfs.exclude";
  String includesFile = HOSTS_TEST_DIR + "/dfs.include";
  private String excludesXmlFile = HOSTS_TEST_DIR + "/dfs.exclude.xml";

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    // Delete test files after running tests
    EXCLUDES_FILE.delete();
    INCLUDES_FILE.delete();

  }

  /*
   * 1.Create dfs.exclude,dfs.include file
   * 2.Write host names per line
   * 3.Write comments starting with #
   * 4.Close file
   * 5.Compare if number of hosts reported by HostsFileReader
   *   are equal to the number of hosts written
   */
  @Test
  public void testHostsFileReader() throws Exception {

    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.write("somehost1\n");
    efw.write("#This-is-comment\n");
    efw.write("somehost2\n");
    efw.write("somehost3 # host3\n");
    efw.write("somehost4\n");
    efw.write("somehost4 somehost5\n");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.write("somehost1\n");
    ifw.write("somehost2\n");
    ifw.write("somehost3\n");
    ifw.write("#This-is-comment\n");
    ifw.write("somehost4 # host4\n");
    ifw.write("somehost4 somehost5\n");
    ifw.close();

    HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

    int includesLen = hfp.getHosts().size();
    int excludesLen = hfp.getExcludedHosts().size();

    assertEquals(5, includesLen);
    assertEquals(5, excludesLen);

    assertTrue(hfp.getHosts().contains("somehost5"));
    assertFalse(hfp.getHosts().contains("host3"));

    assertTrue(hfp.getExcludedHosts().contains("somehost5"));
    assertFalse(hfp.getExcludedHosts().contains("host4"));

    // test for refreshing hostreader wit new include/exclude host files
    String newExcludesFile = HOSTS_TEST_DIR + "/dfs1.exclude";
    String newIncludesFile = HOSTS_TEST_DIR + "/dfs1.include";

    efw = new FileWriter(newExcludesFile);
    ifw = new FileWriter(newIncludesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.write("node1\n");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.write("node2\n");
    ifw.close();

    hfp.refresh(newIncludesFile, newExcludesFile);
    assertTrue(hfp.getExcludedHosts().contains("node1"));
    assertTrue(hfp.getHosts().contains("node2"));

    HostDetails hostDetails = hfp.getHostDetails();
    assertTrue(hostDetails.getExcludedHosts().contains("node1"));
    assertTrue(hostDetails.getIncludedHosts().contains("node2"));
    assertEquals(newIncludesFile, hostDetails.getIncludesFile());
    assertEquals(newExcludesFile, hostDetails.getExcludesFile());
  }

  /*
   * Test creating a new HostsFileReader with nonexistent files
   */
  @Test
  public void testCreateHostFileReaderWithNonexistentFile() throws Exception {
    try {
      new HostsFileReader(
          HOSTS_TEST_DIR + "/doesnt-exist",
          HOSTS_TEST_DIR + "/doesnt-exist");
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException ex) {
      // Exception as expected
    }
  }

  /*
   * Test refreshing an existing HostsFileReader with an includes file that no longer exists
   */
  @Test
  public void testRefreshHostFileReaderWithNonexistentFile() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.close();

    ifw.close();

    HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);
    assertTrue(INCLUDES_FILE.delete());
    try {
      hfp.refresh();
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException ex) {
      // Exception as expected
    }
  }

  /*
   * Test for null file
   */
  @Test
  public void testHostFileReaderWithNull() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.close();

    ifw.close();

    HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

    int includesLen = hfp.getHosts().size();
    int excludesLen = hfp.getExcludedHosts().size();

    // TestCase1: Check if lines beginning with # are ignored
    assertEquals(0, includesLen);
    assertEquals(0, excludesLen);

    // TestCase2: Check if given host names are reported by getHosts and
    // getExcludedHosts
    assertFalse(hfp.getHosts().contains("somehost5"));

    assertFalse(hfp.getExcludedHosts().contains("somehost5"));
  }

  /*
   * Check if only comments can be written to hosts file
   */
  @Test
  public void testHostFileReaderWithCommentsOnly() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.close();

    HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

    int includesLen = hfp.getHosts().size();
    int excludesLen = hfp.getExcludedHosts().size();

    assertEquals(0, includesLen);
    assertEquals(0, excludesLen);

    assertFalse(hfp.getHosts().contains("somehost5"));

    assertFalse(hfp.getExcludedHosts().contains("somehost5"));

  }

  /*
   * Test if spaces are allowed in host names
   */
  @Test
  public void testHostFileReaderWithSpaces() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.write("   somehost somehost2");
    efw.write("   somehost3 # somehost4");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.write("   somehost somehost2");
    ifw.write("   somehost3 # somehost4");
    ifw.close();

    HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

    int includesLen = hfp.getHosts().size();
    int excludesLen = hfp.getExcludedHosts().size();

    assertEquals(3, includesLen);
    assertEquals(3, excludesLen);

    assertTrue(hfp.getHosts().contains("somehost3"));
    assertFalse(hfp.getHosts().contains("somehost5"));
    assertFalse(hfp.getHosts().contains("somehost4"));

    assertTrue(hfp.getExcludedHosts().contains("somehost3"));
    assertFalse(hfp.getExcludedHosts().contains("somehost5"));
    assertFalse(hfp.getExcludedHosts().contains("somehost4"));

  }

  /*
   * Test if spaces , tabs and new lines are allowed
   */
  @Test
  public void testHostFileReaderWithTabs() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.write("     \n");
    efw.write("   somehost \t somehost2 \n somehost4");
    efw.write("   somehost3 \t # somehost5");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.write("     \n");
    ifw.write("   somehost \t  somehost2 \n somehost4");
    ifw.write("   somehost3 \t # somehost5");
    ifw.close();

    HostsFileReader hfp = new HostsFileReader(includesFile, excludesFile);

    int includesLen = hfp.getHosts().size();
    int excludesLen = hfp.getExcludedHosts().size();

    assertEquals(4, includesLen);
    assertEquals(4, excludesLen);

    assertTrue(hfp.getHosts().contains("somehost2"));
    assertFalse(hfp.getHosts().contains("somehost5"));

    assertTrue(hfp.getExcludedHosts().contains("somehost2"));
    assertFalse(hfp.getExcludedHosts().contains("somehost5"));

  }
}
