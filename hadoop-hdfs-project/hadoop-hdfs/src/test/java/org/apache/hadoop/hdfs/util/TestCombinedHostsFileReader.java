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
package org.apache.hadoop.hdfs.util;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for JSON based HostsFileReader.
 */
public class TestCombinedHostsFileReader {

  // Using /test/build/data/tmp directory to store temporary files
  static final String HOSTSTESTDIR = GenericTestUtils.getTestDir()
      .getAbsolutePath();
  private final File newFile = new File(HOSTSTESTDIR, "dfs.hosts.new.json");

  static final String TESTCACHEDATADIR =
      System.getProperty("test.cache.data", "build/test/cache");
  private final File jsonFile = new File(TESTCACHEDATADIR, "dfs.hosts.json");
  private final File legacyFile =
      new File(TESTCACHEDATADIR, "legacy.dfs.hosts.json");

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    // Delete test file after running tests
    newFile.delete();

  }

  /*
   * Load the legacy test json file
   */
  @Test
  public void testLoadLegacyJsonFile() throws Exception {
    DatanodeAdminProperties[] all =
        CombinedHostsFileReader.readFile(legacyFile.getAbsolutePath());
    assertEquals(7, all.length);
  }

  /*
   * Load the test json file
   */
  @Test
  public void testLoadExistingJsonFile() throws Exception {
    DatanodeAdminProperties[] all =
        CombinedHostsFileReader.readFile(jsonFile.getAbsolutePath());
    assertEquals(7, all.length);
  }

  /*
   * Test empty json config file
   */
  @Test
  public void testEmptyCombinedHostsFileReader() throws Exception {
    FileWriter hosts = new FileWriter(newFile);
    hosts.write("");
    hosts.close();
    DatanodeAdminProperties[] all =
        CombinedHostsFileReader.readFile(newFile.getAbsolutePath());
    assertEquals(0, all.length);
  }
}
