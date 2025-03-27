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

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileBaseImpl;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileSystemImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_STORE_FS_ASYNC_THREADS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


/**
 * Test the FileSystem (e.g., HDFS) implementation of the State Store driver.
 */
public class TestStateStoreFileSystem extends TestStateStoreDriverBase {

  private static MiniDFSCluster dfsCluster;

  private String numFsAsyncThreads;

  public void initTestStateStoreFileSystem(String pNumFsAsyncThreads) throws Exception {
    this.numFsAsyncThreads = pNumFsAsyncThreads;
    setupCluster(numFsAsyncThreads);
    removeAll(getStateStoreDriver());
  }

  private static void setupCluster(String numFsAsyncThreads) throws Exception {
    Configuration conf =
        FederationStateStoreTestUtils.getStateStoreConfiguration(StateStoreFileSystemImpl.class);
    conf.set(StateStoreFileSystemImpl.FEDERATION_STORE_FS_PATH, "/hdfs-federation/");
    conf.setInt(FEDERATION_STORE_FS_ASYNC_THREADS, Integer.parseInt(numFsAsyncThreads));

    // Create HDFS cluster to back the state tore
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.numDataNodes(1);
    dfsCluster = builder.build();
    dfsCluster.waitClusterUp();
    getStateStore(conf);
  }

  public static List<String[]> data() {
    return Arrays.asList(new String[][] {{"20"}, {"0"}});
  }

  public void startup() throws Exception {
    setupCluster(numFsAsyncThreads);
    removeAll(getStateStoreDriver());
  }

  @AfterEach
  public void tearDown() throws Exception {
    tearDownCluster();
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testInsert(String pNumFsAsyncThreads)
      throws Exception {
    initTestStateStoreFileSystem(pNumFsAsyncThreads);
    testInsert(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testUpdate(String pNumFsAsyncThreads) throws Exception {
    initTestStateStoreFileSystem(pNumFsAsyncThreads);
    testPut(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testDelete(String pNumFsAsyncThreads)
      throws Exception {
    initTestStateStoreFileSystem(pNumFsAsyncThreads);
    testRemove(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testFetchErrors(String pNumFsAsyncThreads)
      throws Exception {
    initTestStateStoreFileSystem(pNumFsAsyncThreads);
    testFetchErrors(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testMetrics(String pNumFsAsyncThreads)
      throws Exception {
    initTestStateStoreFileSystem(pNumFsAsyncThreads);
    testMetrics(getStateStoreDriver());
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testInsertWithErrorDuringWrite(String pNumFsAsyncThreads)
      throws Exception {
    initTestStateStoreFileSystem(pNumFsAsyncThreads);
    StateStoreFileBaseImpl driver = spy((StateStoreFileBaseImpl)getStateStoreDriver());
    doAnswer((Answer<BufferedWriter>) a -> {
      BufferedWriter writer = (BufferedWriter) a.callRealMethod();
      BufferedWriter spyWriter = spy(writer);
      doThrow(IOException.class).when(spyWriter).write(any(String.class));
      return spyWriter;
    }).when(driver).getWriter(any());

    testInsertWithErrorDuringWrite(driver, MembershipState.class);
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testCacheLoadMetrics(String pNumFsAsyncThreads) throws Exception {
    initTestStateStoreFileSystem(pNumFsAsyncThreads);
    // inject value of CacheMountTableLoad as -1 initially, if tests get CacheMountTableLoadAvgTime
    // value as -1 ms, that would mean no other sample with value >= 0 would have been received and
    // hence this would be failure to assert that mount table avg load time is higher than -1
    getStateStoreService().getMetrics().setCacheLoading("MountTable", -1);
    long curMountTableLoadNum = getMountTableCacheLoadSamples(getStateStoreDriver());
    getStateStoreService().refreshCaches(true);
    getStateStoreService().refreshCaches(true);
    testCacheLoadMetrics(getStateStoreDriver(), curMountTableLoadNum + 2, -1);
  }
}