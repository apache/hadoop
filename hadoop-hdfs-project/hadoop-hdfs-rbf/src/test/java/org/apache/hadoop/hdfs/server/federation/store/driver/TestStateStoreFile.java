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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_STORE_FILE_ASYNC_THREADS;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the FileSystem (e.g., HDFS) implementation of the State Store driver.
 */
@RunWith(Parameterized.class)
public class TestStateStoreFile extends TestStateStoreDriverBase {

  private final String numFileAsyncThreads;

  public TestStateStoreFile(String numFileAsyncThreads) {
    this.numFileAsyncThreads = numFileAsyncThreads;
  }

  @Parameterized.Parameters(name = "numFileAsyncThreads-{0}")
  public static List<String[]> data() {
    return Arrays.asList(new String[][] {{"20"}, {"0"}});
  }

  private static void setupCluster(String numFsAsyncThreads) throws Exception {
    Configuration conf = getStateStoreConfiguration(StateStoreFileImpl.class);
    conf.setInt(FEDERATION_STORE_FILE_ASYNC_THREADS, Integer.parseInt(numFsAsyncThreads));
    getStateStore(conf);
  }

  @Before
  public void startup() throws Exception {
    setupCluster(numFileAsyncThreads);
    removeAll(getStateStoreDriver());
  }

  @After
  public void tearDown() throws Exception {
    tearDownCluster();
  }

  @Test
  public void testInsert()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(getStateStoreDriver());
  }

  @Test
  public void testUpdate()
      throws IllegalArgumentException, ReflectiveOperationException,
      IOException, SecurityException {
    testPut(getStateStoreDriver());
  }

  @Test
  public void testDelete()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testRemove(getStateStoreDriver());
  }

  @Test
  public void testFetchErrors()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testFetchErrors(getStateStoreDriver());
  }

  @Test
  public void testMetrics()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testMetrics(getStateStoreDriver());
  }

  @Test
  public void testCacheLoadMetrics() throws IOException {
    // inject value of CacheMountTableLoad as -1 initially, if tests get CacheMountTableLoadAvgTime
    // value as -1 ms, that would mean no other sample with value >= 0 would have been received and
    // hence this would be failure to assert that mount table avg load time is higher than -1
    getStateStoreService().getMetrics().setCacheLoading("MountTable", -1);
    long curMountTableLoadNum = getMountTableCacheLoadSamples(getStateStoreDriver());
    getStateStoreService().refreshCaches(true);
    testCacheLoadMetrics(getStateStoreDriver(), curMountTableLoadNum + 1, -1);
  }

}