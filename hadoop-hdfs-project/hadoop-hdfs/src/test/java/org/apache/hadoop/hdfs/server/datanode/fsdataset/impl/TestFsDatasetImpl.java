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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestFsDatasetImpl {
  private static final String BASE_DIR =
      System.getProperty("test.build.dir") + "/fsdatasetimpl";
  private static final int NUM_INIT_VOLUMES = 2;

  private DataStorage storage;
  private FsDatasetImpl dataset;

  private static void createStorageDirs(DataStorage storage, Configuration conf,
      int numDirs) throws IOException {
    List<Storage.StorageDirectory> dirs =
        new ArrayList<Storage.StorageDirectory>();
    List<String> dirStrings = new ArrayList<String>();
    for (int i = 0; i < numDirs; i++) {
      String loc = BASE_DIR + "/data" + i;
      dirStrings.add(loc);
      dirs.add(new Storage.StorageDirectory(new File(loc)));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    String dataDir = StringUtils.join(",", dirStrings);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
    when(storage.getNumStorageDirs()).thenReturn(numDirs);
  }

  @Before
  public void setUp() throws IOException {
    final DataNode datanode = Mockito.mock(DataNode.class);
    storage = Mockito.mock(DataStorage.class);
    Configuration conf = new Configuration();
    final DNConf dnConf = new DNConf(conf);

    when(datanode.getConf()).thenReturn(conf);
    when(datanode.getDnConf()).thenReturn(dnConf);

    createStorageDirs(storage, conf, NUM_INIT_VOLUMES);
    dataset = new FsDatasetImpl(datanode, storage, conf);

    assertEquals(NUM_INIT_VOLUMES, dataset.getVolumes().size());
    assertEquals(0, dataset.getNumFailedVolumes());
  }

  @Test
  public void testAddVolumes() throws IOException {
    final int numNewVolumes = 3;
    final int numExistingVolumes = dataset.getVolumes().size();
    final int totalVolumes = numNewVolumes + numExistingVolumes;
    List<StorageLocation> newLocations = new ArrayList<StorageLocation>();
    for (int i = 0; i < numNewVolumes; i++) {
      String path = BASE_DIR + "/newData" + i;
      newLocations.add(StorageLocation.parse(path));
      when(storage.getStorageDir(numExistingVolumes + i))
          .thenReturn(new Storage.StorageDirectory(new File(path)));
    }
    when(storage.getNumStorageDirs()).thenReturn(totalVolumes);

    dataset.addVolumes(newLocations);
    assertEquals(totalVolumes, dataset.getVolumes().size());
    for (int i = 0; i < numNewVolumes; i++) {
      assertEquals(newLocations.get(i).getFile().getPath(),
          dataset.getVolumes().get(numExistingVolumes + i).getBasePath());
    }
  }
}
