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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Helper for writing snapshot related tests
 */
public class SnapshotTestHelper {
  private SnapshotTestHelper() {
    // Cannot be instantinatied
  }

  public static Path getSnapshotRoot(Path snapshottedDir, String snapshotName) {
    return new Path(snapshottedDir, ".snapshot/" + snapshotName);
  }

  public static Path getSnapshotPath(Path snapshottedDir, String snapshotName,
      String fileLocalName) {
    return new Path(getSnapshotRoot(snapshottedDir, snapshotName),
        fileLocalName);
  }

  /**
   * Create snapshot for a dir using a given snapshot name
   * 
   * @param hdfs DistributedFileSystem instance
   * @param snapshottedDir The dir to be snapshotted
   * @param snapshotName The name of the snapshot
   * @return The path of the snapshot root
   */
  public static Path createSnapshot(DistributedFileSystem hdfs,
      Path snapshottedDir, String snapshotName) throws Exception {
    assert hdfs.exists(snapshottedDir);
    hdfs.allowSnapshot(snapshottedDir.toString());
    hdfs.createSnapshot(snapshotName, snapshottedDir.toString());
    return SnapshotTestHelper.getSnapshotRoot(snapshottedDir, snapshotName);
  }

  /**
   * Check the functionality of a snapshot.
   * 
   * @param hdfs DistributedFileSystem instance
   * @param snapshotRoot The root of the snapshot
   * @param snapshottedDir The snapshotted directory
   */
  public static void checkSnapshotCreation(DistributedFileSystem hdfs,
      Path snapshotRoot, Path snapshottedDir) throws Exception {
    // Currently we only check if the snapshot was created successfully
    assertTrue(hdfs.exists(snapshotRoot));
    // Compare the snapshot with the current dir
    FileStatus[] currentFiles = hdfs.listStatus(snapshottedDir);
    FileStatus[] snapshotFiles = hdfs.listStatus(snapshotRoot);
    assertEquals(currentFiles.length, snapshotFiles.length);
  }
}
