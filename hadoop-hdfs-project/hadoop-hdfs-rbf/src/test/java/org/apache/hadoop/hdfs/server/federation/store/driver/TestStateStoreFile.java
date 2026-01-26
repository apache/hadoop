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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the FileSystem (e.g., HDFS) implementation of the State Store driver.
 */
public class TestStateStoreFile extends TestStateStoreDriverBase {

  private String numFileAsyncThreads;

  public void initTestStateStoreFile(String pBumFileAsyncThreads) throws Exception {
    this.numFileAsyncThreads = pBumFileAsyncThreads;
    setupCluster(numFileAsyncThreads);
    removeAll(getStateStoreDriver());
  }

  public static List<String[]> data() {
    return Arrays.asList(new String[][] {{"20"}, {"0"}});
  }

  private static void setupCluster(String numFsAsyncThreads) throws Exception {
    Configuration conf = getStateStoreConfiguration(StateStoreFileImpl.class);
    conf.setInt(FEDERATION_STORE_FILE_ASYNC_THREADS, Integer.parseInt(numFsAsyncThreads));
    getStateStore(conf);
  }

  public void startup() throws Exception {
    setupCluster(numFileAsyncThreads);
    removeAll(getStateStoreDriver());
  }

  @AfterEach
  public void tearDown() throws Exception {
    tearDownCluster();
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testInsert(String pBumFileAsyncThreads)
      throws Exception {
    initTestStateStoreFile(pBumFileAsyncThreads);
    testInsert(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testUpdate(String pBumFileAsyncThreads)
      throws Exception {
    initTestStateStoreFile(pBumFileAsyncThreads);
    testPut(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testDelete(String pBumFileAsyncThreads)
      throws Exception {
    initTestStateStoreFile(pBumFileAsyncThreads);
    testRemove(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testFetchErrors(String pBumFileAsyncThreads)
      throws Exception {
    initTestStateStoreFile(pBumFileAsyncThreads);
    testFetchErrors(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testMetrics(String pBumFileAsyncThreads)
      throws Exception {
    initTestStateStoreFile(pBumFileAsyncThreads);
    testMetrics(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testCacheLoadMetrics(String pBumFileAsyncThreads) throws Exception {
    initTestStateStoreFile(pBumFileAsyncThreads);
    // inject value of CacheMountTableLoad as -1 initially, if tests get CacheMountTableLoadAvgTime
    // value as -1 ms, that would mean no other sample with value >= 0 would have been received and
    // hence this would be failure to assert that mount table avg load time is higher than -1
    getStateStoreService().getMetrics().setCacheLoading("MountTable", -1);
    long curMountTableLoadNum = getMountTableCacheLoadSamples(getStateStoreDriver());
    getStateStoreService().refreshCaches(true);
    testCacheLoadMetrics(getStateStoreDriver(), curMountTableLoadNum + 1, -1);
  }

}