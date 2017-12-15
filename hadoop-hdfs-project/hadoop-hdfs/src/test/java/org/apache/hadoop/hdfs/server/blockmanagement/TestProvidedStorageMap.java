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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.TestProvidedImpl;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.util.RwLock;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class tests the {@link ProvidedStorageMap}.
 */
public class TestProvidedStorageMap {

  private Configuration conf;
  private BlockManager bm;
  private RwLock nameSystemLock;
  private String providedStorageID;
  private String blockPoolID;

  @Before
  public void setup() {
    providedStorageID = DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT;
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
            providedStorageID);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);
    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TestProvidedImpl.TestFileRegionBlockAliasMap.class,
        BlockAliasMap.class);
    blockPoolID = "BP-12344-10.1.1.2-12344";
    bm = mock(BlockManager.class);
    when(bm.getBlockPoolId()).thenReturn(blockPoolID);
    nameSystemLock = mock(RwLock.class);
  }

  private DatanodeDescriptor createDatanodeDescriptor(int port) {
    return DFSTestUtil.getDatanodeDescriptor("127.0.0.1", port, "defaultRack",
        "localhost");
  }

  @Test
  public void testProvidedStorageMap() throws IOException {
    ProvidedStorageMap providedMap = new ProvidedStorageMap(
        nameSystemLock, bm, conf);
    DatanodeStorageInfo providedMapStorage =
        providedMap.getProvidedStorageInfo();
    // the provided storage cannot be null
    assertNotNull(providedMapStorage);

    // create a datanode
    DatanodeDescriptor dn1 = createDatanodeDescriptor(5000);

    // associate two storages to the datanode
    DatanodeStorage dn1ProvidedStorage = new DatanodeStorage(
        providedStorageID,
        DatanodeStorage.State.NORMAL,
        StorageType.PROVIDED);
    DatanodeStorage dn1DiskStorage = new DatanodeStorage(
        "sid-1", DatanodeStorage.State.NORMAL, StorageType.DISK);

    when(nameSystemLock.hasWriteLock()).thenReturn(true);
    DatanodeStorageInfo dns1Provided =
        providedMap.getStorage(dn1, dn1ProvidedStorage);
    DatanodeStorageInfo dns1Disk = providedMap.getStorage(dn1, dn1DiskStorage);

    assertTrue("The provided storages should be equal",
        dns1Provided == providedMapStorage);
    assertTrue("Disk storage has not yet been registered with block manager",
        dns1Disk == null);
    // add the disk storage to the datanode.
    DatanodeStorageInfo dnsDisk = new DatanodeStorageInfo(dn1, dn1DiskStorage);
    dn1.injectStorage(dnsDisk);
    assertTrue("Disk storage must match the injected storage info",
        dnsDisk == providedMap.getStorage(dn1, dn1DiskStorage));

    // create a 2nd datanode
    DatanodeDescriptor dn2 = createDatanodeDescriptor(5010);
    // associate a provided storage with the datanode
    DatanodeStorage dn2ProvidedStorage = new DatanodeStorage(
        providedStorageID,
        DatanodeStorage.State.NORMAL,
        StorageType.PROVIDED);

    DatanodeStorageInfo dns2Provided = providedMap.getStorage(
        dn2, dn2ProvidedStorage);
    assertTrue("The provided storages should be equal",
        dns2Provided == providedMapStorage);
    assertTrue("The DatanodeDescriptor should contain the provided storage",
        dn2.getStorageInfo(providedStorageID) == providedMapStorage);
  }
}
