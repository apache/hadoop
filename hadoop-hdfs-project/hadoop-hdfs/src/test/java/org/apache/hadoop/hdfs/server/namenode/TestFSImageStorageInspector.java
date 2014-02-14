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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getFinalizedEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.junit.Test;

public class TestFSImageStorageInspector {
  /**
   * Simple test with image, edits, and inprogress edits
   */
  @Test
  public void testCurrentStorageInspector() throws IOException {
    FSImageTransactionalStorageInspector inspector = 
        new FSImageTransactionalStorageInspector();
    
    StorageDirectory mockDir = FSImageTestUtil.mockStorageDirectory(
        NameNodeDirType.IMAGE_AND_EDITS,
        false,
        "/foo/current/" + getImageFileName(123),
        "/foo/current/" + getFinalizedEditsFileName(123, 456),
        "/foo/current/" + getImageFileName(456),
        "/foo/current/" + getInProgressEditsFileName(457));

    inspector.inspectDirectory(mockDir);
    assertEquals(2, inspector.foundImages.size());

    FSImageFile latestImage = inspector.getLatestImages().get(0);
    assertEquals(456, latestImage.txId);
    assertSame(mockDir, latestImage.sd);
    assertTrue(inspector.isUpgradeFinalized());
    
    assertEquals(new File("/foo/current/"+getImageFileName(456)), 
        latestImage.getFile());
  }
}
