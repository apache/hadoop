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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test a router end-to-end including the MountTable.
 */
public class TestRouterMountTable {

  private static StateStoreDFSCluster cluster;
  private static NamenodeContext nnContext;
  private static RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static ClientProtocol routerProtocol;

  @BeforeClass
  public static void globalSetUp() throws Exception {

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 1);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    nnContext = cluster.getRandomNamenode();
    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testReadOnly() throws Exception {

    // Add a read only entry
    MountTable readOnlyEntry = MountTable.newInstance(
        "/readonly", Collections.singletonMap("ns0", "/testdir"));
    readOnlyEntry.setReadOnly(true);
    assertTrue(addMountTable(readOnlyEntry));

    // Add a regular entry
    MountTable regularEntry = MountTable.newInstance(
        "/regular", Collections.singletonMap("ns0", "/testdir"));
    assertTrue(addMountTable(regularEntry));

    // Create a folder which should show in all locations
    final FileSystem nnFs = nnContext.getFileSystem();
    final FileSystem routerFs = routerContext.getFileSystem();
    assertTrue(routerFs.mkdirs(new Path("/regular/newdir")));

    FileStatus dirStatusNn =
        nnFs.getFileStatus(new Path("/testdir/newdir"));
    assertTrue(dirStatusNn.isDirectory());
    FileStatus dirStatusRegular =
        routerFs.getFileStatus(new Path("/regular/newdir"));
    assertTrue(dirStatusRegular.isDirectory());
    FileStatus dirStatusReadOnly =
        routerFs.getFileStatus(new Path("/readonly/newdir"));
    assertTrue(dirStatusReadOnly.isDirectory());

    // It should fail writing into a read only path
    try {
      routerFs.mkdirs(new Path("/readonly/newdirfail"));
      fail("We should not be able to write into a read only mount point");
    } catch (IOException ioe) {
      String msg = ioe.getMessage();
      assertTrue(msg.startsWith(
          "/readonly/newdirfail is in a read only mount point"));
    }
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to add.
   * @return If it was succesfully added.
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
    mountTable.loadCache(true);

    return addResponse.getStatus();
  }

  /**
   * Verify that the file/dir listing contains correct date/time information.
   */
  @Test
  public void testListFilesTime() throws Exception {
    Long beforeCreatingTime = Time.now();
    // Add mount table entry
    MountTable addEntry = MountTable.newInstance(
        "/testdir", Collections.singletonMap("ns0", "/testdir"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance(
        "/testdir2", Collections.singletonMap("ns0", "/testdir2"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance(
        "/testdir/subdir", Collections.singletonMap("ns0", "/testdir/subdir"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance(
        "/testdir3/subdir1", Collections.singletonMap("ns0", "/testdir3"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance(
        "/testA/testB/testC/testD", Collections.singletonMap("ns0", "/test"));
    assertTrue(addMountTable(addEntry));

    // Create test dir in NN
    final FileSystem nnFs = nnContext.getFileSystem();
    assertTrue(nnFs.mkdirs(new Path("/newdir")));

    Map<String, Long> pathModTime = new TreeMap<>();
    for (String mount : mountTable.getMountPoints("/")) {
      if (mountTable.getMountPoint("/"+mount) != null) {
        pathModTime.put(mount, mountTable.getMountPoint("/"+mount)
            .getDateModified());
      } else {
        List<MountTable> entries = mountTable.getMounts("/"+mount);
        for (MountTable entry : entries) {
          if (pathModTime.get(mount) == null ||
              pathModTime.get(mount) < entry.getDateModified()) {
            pathModTime.put(mount, entry.getDateModified());
          }
        }
      }
    }
    FileStatus[] iterator = nnFs.listStatus(new Path("/"));
    for (FileStatus file : iterator) {
      pathModTime.put(file.getPath().getName(), file.getModificationTime());
    }
    // Fetch listing
    DirectoryListing listing =
        routerProtocol.getListing("/", HdfsFileStatus.EMPTY_NAME, false);
    Iterator<String> pathModTimeIterator = pathModTime.keySet().iterator();

    // Match date/time for each path returned
    for(HdfsFileStatus f : listing.getPartialListing()) {
      String fileName = pathModTimeIterator.next();
      String currentFile = f.getFullPath(new Path("/")).getName();
      Long currentTime = f.getModificationTime();
      Long expectedTime = pathModTime.get(currentFile);

      assertEquals(currentFile, fileName);
      assertTrue(currentTime > beforeCreatingTime);
      assertEquals(currentTime, expectedTime);
    }
    // Verify the total number of results found/matched
    assertEquals(pathModTime.size(), listing.getPartialListing().length);
  }
}