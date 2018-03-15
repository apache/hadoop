/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock;

import org.apache.hadoop.cblock.meta.VolumeDescriptor;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.cblock.util.MockStorageClient;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Test the CBlock server state is maintained in persistent storage and can be
 * recovered on CBlock server restart.
 */
public class TestCBlockServerPersistence {

  /**
   * Test when cblock server fails with volume meta data, the meta data can be
   * restored correctly.
   * @throws Exception
   */
  @Test
  public void testWriteToPersistentStore() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String userName = "testWriteToPersistentStore";
    String volumeName1 = "testVolume1";
    String volumeName2 = "testVolume2";
    long volumeSize1 = 30L*1024*1024*1024;
    long volumeSize2 = 15L*1024*1024*1024;
    int blockSize = 4096;
    CBlockManager cBlockManager = null;
    CBlockManager cBlockManager1 = null;
    String path = GenericTestUtils
        .getTempPath(TestCBlockServerPersistence.class.getSimpleName());
    File filePath = new File(path);
    if(!filePath.exists() && !filePath.mkdirs()) {
      throw new IOException("Unable to create test DB dir");
    }
    conf.set(DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY, path.concat(
        "/testCblockPersistence.dat"));
    try {
      ScmClient storageClient = new MockStorageClient();
      conf.set(DFS_CBLOCK_SERVICERPC_ADDRESS_KEY, "127.0.0.1:0");
      conf.set(DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY, "127.0.0.1:0");
      cBlockManager = new CBlockManager(conf, storageClient);
      cBlockManager.start();
      cBlockManager.createVolume(userName, volumeName1, volumeSize1, blockSize);
      cBlockManager.createVolume(userName, volumeName2, volumeSize2, blockSize);
      List<VolumeDescriptor> allVolumes = cBlockManager.getAllVolumes();
      // close the cblock server. Since meta data is written to disk on volume
      // creation, closing server here is the same as a cblock server crash.
      cBlockManager.close();
      cBlockManager.stop();
      cBlockManager.join();
      cBlockManager = null;
      assertEquals(2, allVolumes.size());
      VolumeDescriptor volumeDescriptor1 = allVolumes.get(0);
      VolumeDescriptor volumeDescriptor2 = allVolumes.get(1);

      // create a new cblock server instance. This is just the
      // same as restarting cblock server.
      ScmClient storageClient1 = new MockStorageClient();
      OzoneConfiguration conf1 = new OzoneConfiguration();
      conf1.set(DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY, path.concat(
          "/testCblockPersistence.dat"));
      conf1.set(DFS_CBLOCK_SERVICERPC_ADDRESS_KEY, "127.0.0.1:0");
      conf1.set(DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY, "127.0.0.1:0");
      cBlockManager1 = new CBlockManager(conf1, storageClient1);
      cBlockManager1.start();
      List<VolumeDescriptor> allVolumes1 = cBlockManager1.getAllVolumes();
      assertEquals(2, allVolumes1.size());
      VolumeDescriptor newvolumeDescriptor1 = allVolumes1.get(0);
      VolumeDescriptor newvolumeDescriptor2 = allVolumes1.get(1);

      // It seems levelDB iterator gets keys in the same order as keys
      // are inserted, in which case the else clause should never happen.
      // But still kept the second clause if it is possible to get different
      // key ordering from leveldb. And we do not rely on the ordering of keys
      // here.
      if (volumeDescriptor1.getVolumeName().equals(
          newvolumeDescriptor1.getVolumeName())) {
        assertEquals(volumeDescriptor1.toString(),
            newvolumeDescriptor1.toString());
        assertEquals(volumeDescriptor2.toString(),
            newvolumeDescriptor2.toString());
      } else {
        assertEquals(volumeDescriptor1.toString(),
            newvolumeDescriptor2.toString());
        assertEquals(volumeDescriptor2.toString(),
            newvolumeDescriptor1.toString());
      }
    } finally {
      if (cBlockManager != null) {
        cBlockManager.clean();
      }
      if (cBlockManager1 != null) {
        cBlockManager1.close();
        cBlockManager1.stop();
        cBlockManager1.join();
        cBlockManager1.clean();
      }
    }
  }
}
