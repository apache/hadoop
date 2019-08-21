/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.hadoop.ozone.om.ratis.OMRatisSnapshotInfo;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Random;

/**
 * Tests {@link org.apache.hadoop.ozone.om.ratis.OMRatisSnapshotInfo}.
 */
public class TestOMRatisSnapshotInfo {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testSaveAndLoadSnapshotInfo() throws Exception {
    File rootDir = folder.newFolder();
    OMRatisSnapshotInfo omRatisSnapshotInfo = new OMRatisSnapshotInfo(rootDir);

    // Initially term and index should be 0 and -1
    Assert.assertEquals(0, omRatisSnapshotInfo.getTerm());
    Assert.assertEquals(-1, omRatisSnapshotInfo.getIndex());

    Random random = new Random();
    int snapshotIndex = random.nextInt(50);
    int termIndex = random.nextInt(10);

    // Save snapshotInfo to disk
    omRatisSnapshotInfo.updateTerm(termIndex);
    omRatisSnapshotInfo.saveRatisSnapshotToDisk(snapshotIndex);

    Assert.assertEquals(termIndex, omRatisSnapshotInfo.getTerm());
    Assert.assertEquals(snapshotIndex, omRatisSnapshotInfo.getIndex());

    // Load the snapshot file into new SnapshotInfo
    OMRatisSnapshotInfo newSnapshotInfo = new OMRatisSnapshotInfo(rootDir);

    // Verify that the snapshot file loaded properly
    Assert.assertEquals(termIndex, newSnapshotInfo.getTerm());
    Assert.assertEquals(snapshotIndex, newSnapshotInfo.getIndex());
  }

}
