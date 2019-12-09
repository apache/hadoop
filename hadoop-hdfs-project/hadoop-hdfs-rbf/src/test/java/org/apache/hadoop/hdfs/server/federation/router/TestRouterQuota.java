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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

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
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
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
    routerConf.set(RBFConfigKeys.DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL, "2s");

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

    // delete/rename call should be still okay
    routerFs.delete(new Path("/nsquota"), true);
    routerFs.rename(new Path("/nsquota/subdir"), new Path("/nsquota/subdir"));
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
    MountTable updatedMountTable = getMountTable(path);
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
    updatedMountTable = getMountTable(path);
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
  private MountTable getMountTable(String path) throws IOException {
    // Reload the Router cache
    resolver.loadCache(true);
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(path);
    GetMountTableEntriesResponse response = mountTableManager
        .getMountTableEntries(getRequest);
    List<MountTable> results = response.getEntries();

    return !results.isEmpty() ? results.get(0) : null;
  }

  @Test
  public void testQuotaSynchronization() throws IOException {
    long updateNsQuota = 3;
    long updateSsQuota = 4;
    FileSystem nnFs = nnContext1.getFileSystem();
    nnFs.mkdirs(new Path("/testsync"));
    MountTable mountTable = MountTable.newInstance("/quotaSync",
        Collections.singletonMap("ns0", "/testsync"), Time.now(), Time.now());
    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(1)
        .spaceQuota(2).build());
    // Add new mount table
    addMountTable(mountTable);

    // ensure the quota is not set as updated value
    QuotaUsage realQuota = nnContext1.getFileSystem()
        .getQuotaUsage(new Path("/testsync"));
    assertNotEquals(updateNsQuota, realQuota.getQuota());
    assertNotEquals(updateSsQuota, realQuota.getSpaceQuota());

    // Call periodicInvoke to ensure quota  updated in quota manager
    // and state store.
    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    updateService.periodicInvoke();

    mountTable.setQuota(new RouterQuotaUsage.Builder().quota(updateNsQuota)
        .spaceQuota(updateSsQuota).build());
    UpdateMountTableEntryRequest updateRequest = UpdateMountTableEntryRequest
        .newInstance(mountTable);
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    mountTableManager.updateMountTableEntry(updateRequest);

    // verify if the quota is updated in real path
    realQuota = nnContext1.getFileSystem().getQuotaUsage(
        new Path("/testsync"));
    assertEquals(updateNsQuota, realQuota.getQuota());
    assertEquals(updateSsQuota, realQuota.getSpaceQuota());

    // Clear the quota
    mountTable.setQuota(new RouterQuotaUsage.Builder()
        .quota(HdfsConstants.QUOTA_RESET)
        .spaceQuota(HdfsConstants.QUOTA_RESET).build());

    updateRequest = UpdateMountTableEntryRequest
        .newInstance(mountTable);
    client = routerContext.getAdminClient();
    mountTableManager = client.getMountTableManager();
    mountTableManager.updateMountTableEntry(updateRequest);

    // verify if the quota is updated in real path
    realQuota = nnContext1.getFileSystem().getQuotaUsage(
        new Path("/testsync"));
    assertEquals(HdfsConstants.QUOTA_RESET, realQuota.getQuota());
    assertEquals(HdfsConstants.QUOTA_RESET, realQuota.getSpaceQuota());
  }

  @Test
  public void testQuotaRefreshAfterQuotaExceed() throws Exception {
    long nsQuota = 3;
    long ssQuota = 100;
    final FileSystem nnFs1 = nnContext1.getFileSystem();
    final FileSystem nnFs2 = nnContext2.getFileSystem();

    // Add two mount tables:
    // /setquota1 --> ns0---testdir11
    // /setquota2 --> ns1---testdir12
    nnFs1.mkdirs(new Path("/testdir11"));
    nnFs2.mkdirs(new Path("/testdir12"));
    MountTable mountTable1 = MountTable.newInstance("/setquota1",
        Collections.singletonMap("ns0", "/testdir11"));
    mountTable1
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable1);

    MountTable mountTable2 = MountTable.newInstance("/setquota2",
        Collections.singletonMap("ns1", "/testdir12"));
    mountTable2
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable2);

    final FileSystem routerFs = routerContext.getFileSystem();
    // Create directory to make directory count equals to nsQuota
    routerFs.mkdirs(new Path("/setquota1/" + UUID.randomUUID()));
    routerFs.mkdirs(new Path("/setquota1/" + UUID.randomUUID()));

    // create one more directory to exceed the nsQuota
    routerFs.mkdirs(new Path("/setquota1/" + UUID.randomUUID()));

    RouterQuotaUpdateService updateService = routerContext.getRouter()
        .getQuotaCacheUpdateService();
    // Call RouterQuotaUpdateService#periodicInvoke to update quota cache
    updateService.periodicInvoke();
    // Reload the Router cache
    resolver.loadCache(true);

    RouterQuotaManager quotaManager =
        routerContext.getRouter().getQuotaManager();
    ClientProtocol client1 = nnContext1.getClient().getNamenode();
    ClientProtocol client2 = nnContext2.getClient().getNamenode();
    QuotaUsage quota1 = client1.getQuotaUsage("/testdir11");
    QuotaUsage quota2 = client2.getQuotaUsage("/testdir12");
    QuotaUsage cacheQuota1 = quotaManager.getQuotaUsage("/setquota1");
    QuotaUsage cacheQuota2 = quotaManager.getQuotaUsage("/setquota2");

    // Verify quota usage
    assertEquals(4, quota1.getFileAndDirectoryCount());
    assertEquals(4, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(1, quota2.getFileAndDirectoryCount());
    assertEquals(1, cacheQuota2.getFileAndDirectoryCount());

    try {
      // create new directory to trigger NSQuotaExceededException
      routerFs.mkdirs(new Path("/testdir11/" + UUID.randomUUID()));
      fail("Mkdir should be failed under dir /testdir11.");
    } catch (NSQuotaExceededException ignored) {
    }

    // Create directory under the other mount point
    routerFs.mkdirs(new Path("/setquota2/" + UUID.randomUUID()));
    routerFs.mkdirs(new Path("/setquota2/" + UUID.randomUUID()));

    // Call RouterQuotaUpdateService#periodicInvoke to update quota cache
    updateService.periodicInvoke();

    quota1 = client1.getQuotaUsage("/testdir11");
    cacheQuota1 = quotaManager.getQuotaUsage("/setquota1");
    quota2 = client2.getQuotaUsage("/testdir12");
    cacheQuota2 = quotaManager.getQuotaUsage("/setquota2");

    // Verify whether quota usage cache is update by periodicInvoke().
    assertEquals(4, quota1.getFileAndDirectoryCount());
    assertEquals(4, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(3, quota2.getFileAndDirectoryCount());
    assertEquals(3, cacheQuota2.getFileAndDirectoryCount());
  }

  /**
   * Verify whether mount table and quota usage cache is updated properly.
   * {@link RouterQuotaUpdateService#periodicInvoke()} should be able to update
   * the cache and the mount table even if the destination directory for some
   * mount entry is not present in the filesystem.
   */
  @Test
  public void testQuotaRefreshWhenDestinationNotPresent() throws Exception {
    long nsQuota = 5;
    long ssQuota = 3*BLOCK_SIZE;
    final FileSystem nnFs = nnContext1.getFileSystem();

    // Add three mount tables:
    // /setdir1 --> ns0---testdir13
    // /setdir2 --> ns0---testdir14
    // Create destination directory
    nnFs.mkdirs(new Path("/testdir13"));
    nnFs.mkdirs(new Path("/testdir14"));

    MountTable mountTable = MountTable.newInstance("/setdir1",
        Collections.singletonMap("ns0", "/testdir13"));
    mountTable
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    mountTable = MountTable.newInstance("/setdir2",
        Collections.singletonMap("ns0", "/testdir14"));
    mountTable
        .setQuota(new RouterQuotaUsage.Builder().quota(nsQuota)
        .spaceQuota(ssQuota).build());
    addMountTable(mountTable);

    final DFSClient routerClient = routerContext.getClient();
    // Create file
    routerClient.create("/setdir1/file1", true).close();
    routerClient.create("/setdir2/file2", true).close();
    // append data to the file
    appendData("/setdir1/file1", routerClient, BLOCK_SIZE);
    appendData("/setdir2/file2", routerClient, BLOCK_SIZE);

    RouterQuotaUpdateService updateService =
        routerContext.getRouter().getQuotaCacheUpdateService();
    // Update quota cache
    updateService.periodicInvoke();
    // Reload the Router cache
    resolver.loadCache(true);

    ClientProtocol client1 = nnContext1.getClient().getNamenode();
    RouterQuotaManager quotaManager =
        routerContext.getRouter().getQuotaManager();
    QuotaUsage quota1 = client1.getQuotaUsage("/testdir13");
    QuotaUsage quota2 = client1.getQuotaUsage("/testdir14");
    QuotaUsage cacheQuota1 = quotaManager.getQuotaUsage("/setdir1");
    QuotaUsage cacheQuota2 = quotaManager.getQuotaUsage("/setdir2");

    // Get quota details in mount table
    MountTable updatedMountTable = getMountTable("/setdir1");
    RouterQuotaUsage mountQuota1 = updatedMountTable.getQuota();
    updatedMountTable = getMountTable("/setdir2");
    RouterQuotaUsage mountQuota2 = updatedMountTable.getQuota();

    // Verify quota usage
    assertEquals(2, quota1.getFileAndDirectoryCount());
    assertEquals(2, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(2, mountQuota1.getFileAndDirectoryCount());
    assertEquals(2, quota2.getFileAndDirectoryCount());
    assertEquals(2, cacheQuota2.getFileAndDirectoryCount());
    assertEquals(2, mountQuota2.getFileAndDirectoryCount());
    assertEquals(BLOCK_SIZE, quota1.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, cacheQuota1.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, mountQuota1.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, quota2.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, cacheQuota2.getSpaceConsumed());
    assertEquals(BLOCK_SIZE, mountQuota2.getSpaceConsumed());

    FileSystem routerFs = routerContext.getFileSystem();
    // Remove destination directory for the mount entry
    routerFs.delete(new Path("/setdir1"), true);

    // Create file
    routerClient.create("/setdir2/file3", true).close();
    // append data to the file
    appendData("/setdir2/file3", routerClient, BLOCK_SIZE);
    int updatedSpace = BLOCK_SIZE + BLOCK_SIZE;

    // Update quota cache
    updateService.periodicInvoke();

    quota2 = client1.getQuotaUsage("/testdir14");
    cacheQuota1 = quotaManager.getQuotaUsage("/setdir1");
    cacheQuota2 = quotaManager.getQuotaUsage("/setdir2");

    // Get quota details in mount table
    updatedMountTable = getMountTable("/setdir1");
    mountQuota1 = updatedMountTable.getQuota();
    updatedMountTable = getMountTable("/setdir2");
    mountQuota2 = updatedMountTable.getQuota();

    // If destination is not present the quota usage should be reset to 0
    assertEquals(0, cacheQuota1.getFileAndDirectoryCount());
    assertEquals(0, mountQuota1.getFileAndDirectoryCount());
    assertEquals(0, cacheQuota1.getSpaceConsumed());
    assertEquals(0, mountQuota1.getSpaceConsumed());

    // Verify current quota usage for other mount entries
    assertEquals(3, quota2.getFileAndDirectoryCount());
    assertEquals(3, cacheQuota2.getFileAndDirectoryCount());
    assertEquals(3, mountQuota2.getFileAndDirectoryCount());
    assertEquals(updatedSpace, quota2.getSpaceConsumed());
    assertEquals(updatedSpace, cacheQuota2.getSpaceConsumed());
    assertEquals(updatedSpace, mountQuota2.getSpaceConsumed());
  }
}