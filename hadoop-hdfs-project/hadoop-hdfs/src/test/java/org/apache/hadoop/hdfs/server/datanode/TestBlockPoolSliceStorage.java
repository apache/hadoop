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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Test that BlockPoolSliceStorage can correctly generate trash and
 * restore directories for a given block file path.
*/
public class TestBlockPoolSliceStorage {
  public static final Log LOG = LogFactory.getLog(TestBlockPoolSliceStorage.class);

  final Random rand = new Random();
  BlockPoolSliceStorage storage;

  /**
   * BlockPoolSliceStorage with a dummy storage directory. The directory
   * need not exist. We need to extend BlockPoolSliceStorage so we can
   * call {@link Storage#addStorageDir}.
   */
  private static class StubBlockPoolSliceStorage extends BlockPoolSliceStorage {
    StubBlockPoolSliceStorage(int namespaceID, String bpID, long cTime,
                              String clusterId) {
      super(namespaceID, bpID, cTime, clusterId);
      addStorageDir(new StorageDirectory(new File("/tmp/dontcare/" + bpID)));
      assertThat(storageDirs.size(), is(1));
    }
  }

  private String makeRandomIpAddress() {
    return rand.nextInt(256) + "." +
           rand.nextInt(256) + "." +
           rand.nextInt(256) + "." +
           rand.nextInt(256);
  }

  private String makeRandomBlockpoolId() {
    return "BP-" + rand.nextInt(Integer.MAX_VALUE) +
           "-" + makeRandomIpAddress() +
           "-" + rand.nextInt(Integer.MAX_VALUE);
  }

  private BlockPoolSliceStorage makeBlockPoolStorage() {
    return new StubBlockPoolSliceStorage(
        rand.nextInt(Integer.MAX_VALUE),
        makeRandomBlockpoolId(),
        rand.nextInt(Integer.MAX_VALUE),
        UUID.randomUUID().toString());
  }

  private String makeRandomBlockFileSubdir(int nestingLevel) {
    StringBuilder sb = new StringBuilder();

    sb.append(File.separator);

    for (int i = 0; i < nestingLevel; ++i) {
      sb.append("subdir" + rand.nextInt(64) + File.separator);
    }
    return sb.toString();
  }

  /**
   * Test conversion from a block file path to its target trash
   * directory.
   */
  public void getTrashDirectoryForBlockFile(String fileName, int nestingLevel) {
    final String blockFileSubdir = makeRandomBlockFileSubdir(nestingLevel);
    final String blockFileName = fileName;

    String testFilePath =
        storage.getSingularStorageDir().getRoot() + File.separator +
            Storage.STORAGE_DIR_CURRENT +
            blockFileSubdir + blockFileName;

    String expectedTrashPath =
        storage.getSingularStorageDir().getRoot() + File.separator +
            BlockPoolSliceStorage.TRASH_ROOT_DIR +
            blockFileSubdir.substring(0, blockFileSubdir.length() - 1);

    LOG.info("Got subdir " + blockFileSubdir);
    LOG.info("Generated file path " + testFilePath);

    ReplicaInfo info = Mockito.mock(ReplicaInfo.class);
    Mockito.when(info.getBlockURI()).thenReturn(new File(testFilePath).toURI());
    assertThat(storage.getTrashDirectory(info), is(expectedTrashPath));
  }

  /*
   * Test conversion from a block file in a trash directory to its
   * target directory for restore.
  */
  public void getRestoreDirectoryForBlockFile(String fileName, int nestingLevel) {
    BlockPoolSliceStorage storage = makeBlockPoolStorage();
    final String blockFileSubdir = makeRandomBlockFileSubdir(nestingLevel);
    final String blockFileName = fileName;

    String deletedFilePath =
        storage.getSingularStorageDir().getRoot() + File.separator +
        BlockPoolSliceStorage.TRASH_ROOT_DIR +
        blockFileSubdir + blockFileName;

    String expectedRestorePath =
        storage.getSingularStorageDir().getRoot() + File.separator +
            Storage.STORAGE_DIR_CURRENT +
            blockFileSubdir.substring(0, blockFileSubdir.length() - 1);

    LOG.info("Generated deleted file path " + deletedFilePath);
    assertThat(storage.getRestoreDirectory(new File(deletedFilePath)),
               is(expectedRestorePath));

  }

  @Test (timeout=300000)
  public void testGetTrashAndRestoreDirectories() {
    storage = makeBlockPoolStorage();

    // Test a few different nesting levels since block files
    // could be nested such as subdir1/subdir5/blk_...
    // Make sure all nesting levels are handled correctly.
    for (int i = 0; i < 3; ++i) {
      getTrashDirectoryForBlockFile("blk_myblockfile", i);
      getTrashDirectoryForBlockFile("blk_myblockfile.meta", i);
      getRestoreDirectoryForBlockFile("blk_myblockfile", i);
      getRestoreDirectoryForBlockFile("blk_myblockfile.meta", i);
    }
  }
}
