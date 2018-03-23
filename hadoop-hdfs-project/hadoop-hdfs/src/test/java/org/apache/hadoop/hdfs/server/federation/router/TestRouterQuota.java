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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;

/**
 * Tests quota behaviors in Router-based Federation.
 */
public class TestRouterQuota {
  private static StateStoreDFSCluster cluster;
  private static NamenodeContext nnContext1;
  private static NamenodeContext nnContext2;
  private static RouterContext routerContext;
  private static MountTableResolver resolver;

  private static final int BLOCK_SIZE = 512;

  @Before
  public void setUp() throws Exception {

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .quota()
        .rpc()
        .build();
    routerConf.set(DFSConfigKeys.DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL, "2s");

    // override some hdfs settings that used in testing space quota
    Configuration hdfsConf = new Configuration(false);
    hdfsConf.setInt(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    hdfsConf.setInt(HdfsClientConfigKeys.DFS_REPLICATION_KEY, 1);

    cluster.addRouterOverrides(routerConf);
    cluster.addNamenodeOverrides(hdfsConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    nnContext1 = cluster.getNamenode(cluster.getNameservices().get(0), null);
    nnContext2 = cluster.getNamenode(cluster.getNameservices().get(1), null);
    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    resolver = (MountTableResolver) router.getSubclusterResolver();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testNamespaceQuotaExceed() throws Exception {
    long nsQuota = 3;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /nsquota --> ns0---testdir1
    // /nsquota/subdir --> ns1---testdir2
    nnFs1.mkdirs(new Path("/testdir1"));
    nnFs2.mkdirs(new Path("/testdir2"));
    MountTable mountTable1 = MountTable.newInstance("/nsquota",
        Collections.singletonMap("ns0", "/testdir1"));

    mountTable1.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/nsquota/subdir",
        Collections.singletonMap("ns1", "/testdir2"));
    mountTable2.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota).build());
    addMountTable(mountTable2);

    final FileSystem routerFs = routerContext.getFileSystem();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        boolean isNsQuotaViolated = false;
        try {
          // create new directory to trigger NSQuotaExceededException
          routerFs.mkdirs(new Path("/nsquota/" + UUID.randomUUID()));
          routerFs.mkdirs(new Path("/nsquota/subdir/" + UUID.randomUUID()));
        } catch (NSQuotaExceededException e) {
          isNsQuotaViolated = true;
        } catch (IOException ignored) {
        }
        return isNsQuotaViolated;
      }
    }, 5000, 60000);
    // mkdir in real FileSystem should be okay
    nnFs1.mkdirs(new Path("/testdir1/" + UUID.randomUUID()));
    nnFs2.mkdirs(new Path("/testdir2/" + UUID.randomUUID()));
  }

  @Test
  public void testStorageSpaceQuotaaExceed() throws Exception {
    long ssQuota = 3071;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /ssquota --> ns0---testdir3
    // /ssquota/subdir --> ns1---testdir4
    nnFs1.mkdirs(new Path("/testdir3"));
    nnFs2.mkdirs(new Path("/testdir4"));
    MountTable mountTable1 = MountTable.newInstance("/ssquota",
        Collections.singletonMap("ns0", "/testdir3"));

    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/ssquota/subdir",
        Collections.singletonMap("ns1", "/testdir4"));
    mountTable2
        .setQuota(new RouterQuotaUsage.Builder().spaceQuota(ssQuota).build());
    addMountTable(mountTable2);

    DFSClient routerClient = routerContext.getClient();
    routerClient.create("/ssquota/file", true).close();
    routerClient.create("/ssquota/subdir/file", true).close();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        boolean isDsQuotaViolated = false;
        try {
          // append data to trigger NSQuotaExceededException
          appendData("/ssquota/file", routerClient, BLOCK_SIZE);
          appendData("/ssquota/subdir/file", routerClient, BLOCK_SIZE);
        } catch (DSQuotaExceededException e) {
          isDsQuotaViolated = true;
        } catch (IOException ignored) {
        }
        return isDsQuotaViolated;
      }
    }, 5000, 60000);

    // append data to destination path in real FileSystem should be okay
    appendData("/testdir3/file", nnContext1.getClient(), BLOCK_SIZE);
    appendData("/testdir4/file", nnContext2.getClient(), BLOCK_SIZE);
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to add.
   * @return If it was successfully added.
   * @throws IOException Problems adding entries.
   */
  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);

    // Reload the Router cache
    resolver.loadCache(true);

    return addResponse.getStatus();
  }

  /**
   * Append data in specified file.
   * @param path Path of file.
   * @param client DFS Client.
   * @param dataLen The length of write data.
   * @throws IOException
   */
  private void appendData(String path, DFSClient client, int dataLen)
      throws IOException {
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.APPEND);
    HdfsDataOutputStream stream = client.append(path, 1024, createFlag, null,
        null);
    byte[] data = new byte[dataLen];
    stream.write(data);
    stream.close();
  }

  @Test
  public void testSetQuota() throws Exception {
    long nsQuota = 5;
    long ssQuota = 100;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /setquota --> ns0---testdir5
    // /setquota/subdir --> ns1---testdir6
    nnFs1.mkdirs(new Path("/testdir5"));
    nnFs2.mkdirs(new Path("/testdir6"));
    MountTable mountTable1 = MountTable.newInstance("/setquota",
        Collections.singletonMap("ns0", "/testdir5"));
    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    // don't set quota for subpath of mount table
    MountTable mountTable2 = MountTable.newInstance("/setquota/subdir",
        Collections.singletonMap("ns1", "/testdir6"));
    addMountTable(mountTable2);

    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    // ensure setQuota RPC call was invoked
    updateService.periodicInvoke();

    ClientProtocol client1 = nnContext1.getClient().getNamenode();
    ClientProtocol client2 = nnContext2.getClient().getNamenode();
    final QuotaUsage quota1 = client1.getQuotaUsage("/testdir5");
    final QuotaUsage quota2 = client2.getQuotaUsage("/testdir6");

    assertEquals(nsQuota, quota1.getQuota());
    assertEquals(ssQuota, quota1.getSpaceQuota());
    assertEquals(nsQuota, quota2.getQuota());
    assertEquals(ssQuota, quota2.getSpaceQuota());
  }

  @Test
  public void testGetQuota() throws Exception {
    long nsQuota = 10;
    long ssQuota = 100;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /getquota --> ns0---/testdir7
    // /getquota/subdir1 --> ns0---/testdir7/subdir
    // /getquota/subdir2 --> ns1---/testdir8
    nnFs1.mkdirs(new Path("/testdir7"));
    nnFs1.mkdirs(new Path("/testdir7/subdir"));
    nnFs2.mkdirs(new Path("/testdir8"));
    MountTable mountTable1 = MountTable.newInstance("/getquota",
        Collections.singletonMap("ns0", "/testdir7"));
    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/getquota/subdir1",
        Collections.singletonMap("ns0", "/testdir7/subdir"));
    addMountTable(mountTable2);

    MountTable mountTable3 = MountTable.newInstance("/getquota/subdir2",
        Collections.singletonMap("ns1", "/testdir8"));
    addMountTable(mountTable3);

    // use router client to create new files
    DFSClient routerClient = routerContext.getClient();
    routerClient.create("/getquota/file", true).close();
    routerClient.create("/getquota/subdir1/file", true).close();
    routerClient.create("/getquota/subdir2/file", true).close();

    ClientProtocol clientProtocol = routerContext.getClient().getNamenode();
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();
    final QuotaUsage quota = clientProtocol.getQuotaUsage("/getquota");
    // the quota should be aggregated
    assertEquals(6, quota.getFileAndDirectoryCount());
  }

  @Test
  public void testStaleQuotaRemoving() throws Exception {
    long nsQuota = 20;
    long ssQuota = 200;
    String stalePath = "/stalequota";
    final FileSystem nnFs1 = nnContext1.getFileSystem();

    // Add one mount tables:
    // /stalequota --> ns0---/testdir9
    nnFs1.mkdirs(new Path("/testdir9"));
    MountTable mountTable = MountTable.newInstance(stalePath,
        Collections.singletonMap("ns0", "/testdir9"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    // Call periodicInvoke to ensure quota for stalePath was
    // loaded into quota manager.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    // use quota manager to get its quota usage and do verification
    RouterQuotaManager quotaManager = routerContext.getRouter()
        .getQuotaManager();
    RouterQuotaUsage quota = quotaManager.getQuotaUsage(stalePath);
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());

    // remove stale path entry
    removeMountTable(stalePath);
    updateService.periodicInvoke();
    // the stale entry should be removed and we will get null
    quota = quotaManager.getQuotaUsage(stalePath);
    assertNull(quota);
  }

  /**
   * Remove a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to remove.
   * @return If it was successfully removed.
   * @throws IOException Problems removing entries.
   */
  private boolean removeMountTable(String path) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    RemoveMountTableEntryRequest removeRequest = RemoveMountTableEntryRequest
        .newInstance(path);
    RemoveMountTableEntryResponse removeResponse = mountTableManager
        .removeMountTableEntry(removeRequest);

    // Reload the Router cache
    resolver.loadCache(true);
    return removeResponse.getStatus();
  }

  @Test
  public void testQuotaUpdating() throws Exception {
    long nsQuota = 30;
    long ssQuota = 1024;
    String path = "/updatequota";
    final FileSystem nnFs1 = nnContext1.getFileSystem();

    // Add one mount table:
    // /updatequota --> ns0---/testdir10
    nnFs1.mkdirs(new Path("/testdir10"));
    MountTable mountTable = MountTable.newInstance(path,
        Collections.singletonMap("ns0", "/testdir10"));
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    // Call periodicInvoke to ensure quota  updated in quota manager
    // and state store.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    // verify initial quota value
    List<MountTable> results = getMountTable(path);
    MountTable updatedMountTable = !results.isEmpty() ? results.get(0) : null;
    RouterQuotaUsage quota = updatedMountTable.getQuota();
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());
    assertEquals(1, quota.getFileAndDirectoryCount());
    assertEquals(0, quota.getSpaceConsumed());

    // mkdir and write a new file
    final FileSystem routerFs = routerContext.getFileSystem();
    routerFs.mkdirs(new Path(path + "/" + UUID.randomUUID()));
    DFSClient routerClient = routerContext.getClient();
    routerClient.create(path + "/file", true).close();
    appendData(path + "/file", routerClient, BLOCK_SIZE);

    updateService.periodicInvoke();
    results = getMountTable(path);
    updatedMountTable = !results.isEmpty() ? results.get(0) : null;
    quota = updatedMountTable.getQuota();

    // verify if quota has been updated in state store
    assertEquals(nsQuota, quota.getQuota());
    assertEquals(ssQuota, quota.getSpaceQuota());
    assertEquals(3, quota.getFileAndDirectoryCount());
    assertEquals(BLOCK_SIZE, quota.getSpaceConsumed());
  }

  /**
   * Get the mount table entries of specified path through the admin API.
   * @param path Mount table entry to get.
   * @return If it was successfully got.
   * @throws IOException Problems getting entries.
   */
  private List<MountTable> getMountTable(String path) throws IOException {
    // Reload the Router cache
    resolver.loadCache(true);
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(path);
    GetMountTableEntriesResponse removeResponse = mountTableManager
        .getMountTableEntries(getRequest);

    return removeResponse.getEntries();
  }
}
