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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.SyncMountManager;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Test SyncServiceSatisfier.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestSyncServiceSatisfier {

  @Mock
  private BlockManager blockManagerMock;

  @Mock
  private FSNamesystem namesystemMock;

  @Mock
  private SyncMountManager syncMountManagerMock;

  @Mock
  private FSDirectory fsDirectoryMock;

  @Mock
  private INode iNodeMock;

  @Mock
  private INodeDirectory iNodeDirectoryMock;

  @Mock
  private Snapshot snapshotMock;

  @Mock
  private INodeFile iNodeFileMock;

  @Test
  public void cancelCurrentAndScheduleFullResync() throws URISyntaxException,
      IOException {
    Configuration conf = new Configuration();
    UUID syncMountId = UUID.randomUUID();
    String localPath = "/localPath";
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        localPath, "", MountMode.WRITEBACK);
    when(namesystemMock.getBlockManager()).thenReturn(blockManagerMock);
    when(namesystemMock.getFSDirectory()).thenReturn(fsDirectoryMock);
    when(fsDirectoryMock.getINode(any())).thenReturn(iNodeMock);
    when(iNodeMock.asDirectory()).thenReturn(iNodeDirectoryMock);
    when(iNodeMock.asFile()).thenReturn(iNodeFileMock);
    when(iNodeDirectoryMock.getSnapshot(any())).thenReturn(snapshotMock);
    when(syncMountManagerMock.getSyncMounts())
        .thenReturn(Lists.newArrayList(syncMount));

    String source = "sourcePath";
    String sourceCRC = "." + source + ".crc";
    DiffReportEntry entry = new DiffReportEntry(
        SnapshotDiffReport.INodeType.FILE, SnapshotDiffReport.DiffType.CREATE,
        source.getBytes());
    SnapshotDiffReport diffReport =
        new SnapshotDiffReport("snapshotRoot", "fromSnapshot",
        "toSnapshot", Lists.newArrayList(entry));
    when(syncMountManagerMock.makeSnapshotAndPerformDiff(localPath))
        .thenReturn(diffReport);

    SyncServiceSatisfier syncServiceSatisfier =
        new SyncServiceSatisfier(namesystemMock, syncMountManagerMock, conf);
    syncServiceSatisfier.start();

    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();

    boolean cancelled = syncServiceSatisfier.
        cancelCurrentAndScheduleFullResync(syncMountId.toString());
    assertThat(cancelled).isTrue();

    Path testPath  = new Path(PathUtils.getTestDirName(getClass()));
    Path sourcePathParent =
        testPath.getParent().getParent().getParent().getParent().getParent();
    new File(new Path(sourcePathParent, source).toString()).delete();
    new File(new Path(sourcePathParent, sourceCRC).toString()).delete();
  }
}