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

import java.util.Set;

import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
 * Test for JSON based HostsFileReader
 */
public class TestCombinedHostsFileReader {

  // Using /test/build/data/tmp directory to store temporary files
  static final String HOSTS_TEST_DIR = GenericTestUtils.getTestDir()
      .getAbsolutePath();
  File NEW_FILE = new File(HOSTS_TEST_DIR, "dfs.hosts.new.json");

  static final String TEST_CACHE_DATA_DIR =
      System.getProperty("test.cache.data", "build/test/cache");
  File EXISTING_FILE = new File(TEST_CACHE_DATA_DIR, "dfs.hosts.json");

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    // Delete test file after running tests
    NEW_FILE.delete();

  }

  /*
   * Load the existing test json file
   */
  @Test
  public void testLoadExistingJsonFile() throws Exception {
    Set<DatanodeAdminProperties> all =
        CombinedHostsFileReader.readFile(EXISTING_FILE.getAbsolutePath());
    assertEquals(5, all.size());
  }

  /*
   * Test empty json config file
   */
  @Test
  public void testEmptyCombinedHostsFileReader() throws Exception {
    FileWriter hosts = new FileWriter(NEW_FILE);
    hosts.write("");
    hosts.close();
    Set<DatanodeAdminProperties> all =
        CombinedHostsFileReader.readFile(NEW_FILE.getAbsolutePath());
    assertEquals(0, all.size());
  }
}
