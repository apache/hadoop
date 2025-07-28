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
import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

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

  @Mock
  private Callable<DatanodeAdminProperties[]> callable;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @AfterEach
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

  /*
   * When timeout is enabled, test for success when reading file within timeout
   * limits
   */
  @Test
  public void testReadFileWithTimeoutSuccess() throws Exception {

    DatanodeAdminProperties[] all = CombinedHostsFileReader.readFileWithTimeout(
        jsonFile.getAbsolutePath(), 1000);
    assertEquals(7, all.length);
  }

  /*
   * When timeout is enabled, test for IOException when reading file exceeds
   * timeout limits
   */
  @Test
  public void testReadFileWithTimeoutTimeoutException() throws Exception {
    when(callable.call()).thenAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(2000);
        return null;
      }
    });
    assertThrows(IOException.class, () -> {
      CombinedHostsFileReader.readFileWithTimeout(
          jsonFile.getAbsolutePath(), 1);
    });
  }

  /*
   * When timeout is enabled, test for IOException when execution is interrupted
   */
  @Test
  public void testReadFileWithTimeoutInterruptedException() throws Exception {
    when(callable.call()).thenAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new InterruptedException();
      }
    });
    assertThrows(IOException.class, () -> {
      CombinedHostsFileReader.readFileWithTimeout(
          jsonFile.getAbsolutePath(), 1);
    });
  }
}
