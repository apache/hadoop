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
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.INodeType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.RenameEntryWithTempName;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.TranslatedEntry;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Test PhasedPlanFactory.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestPhasedPlanFactory {

  private static final PermissionStatus PERM =
      PermissionStatus.createImmutable("user", "group",
          FsPermission.createImmutable((short) 0));

  @Mock
  private FilePlanner filePlannerMock;
  @Mock
  private DirectoryPlanner directoryPlannerMock;

  private PhasedPlanFactory phasedPlanFactory;

  @Before
  public void setup() {
    this.phasedPlanFactory = new PhasedPlanFactory(filePlannerMock,
        directoryPlannerMock);
  }

  @Test
  public void createCreateSyncTaskTreeTestUnsupportedINodeType()
      throws URISyntaxException {
    String sourcePath = "sourcePath";
    DiffReportEntry entry = new DiffReportEntry(INodeType.SYMLINK,
        DiffType.CREATE, sourcePath.getBytes());
    TranslatedEntry createEntry = TranslatedEntry.withNoRename(entry);
    int targetSnapshotId = 42;
    UUID syncMountId = UUID.randomUUID();
    Path localPath = new Path("localPath");
    URI remoteLocation = new URI("remoteLocation");
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        localPath.toString(), remoteLocation.toString(), MountMode.BACKUP);
    FileAndDirsSyncTasks actualPlan = this.phasedPlanFactory
        .createCreateSyncTaskTree(createEntry, syncMount, targetSnapshotId);

    assertThat(actualPlan.getDirTasks()).isEmpty();
    assertThat(actualPlan.getFileTasks()).isEmpty();
  }

  @Test
  public void createFromPartitionedDiffReportRenames()
      throws URISyntaxException, IOException {
    URI remoteLocation = new URI("remoteLocation/");
    Path localPath = new Path("localPath");
    UUID syncMountId = UUID.randomUUID();
    String sourcePath = "sourcePath";
    String targetPath = "targetPath";
    String iNodeName = localPath.toString() + "/" + sourcePath;
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        localPath.toString(), remoteLocation.toString(), MountMode.BACKUP);
    Optional<Integer> sourceSnapshotId = Optional.of(41);
    String snapshot = "snapshot-1";
    int targetSnapshotId = 42;
    DiffReportEntry entry =
        new DiffReportEntry(INodeType.FILE, DiffType.RENAME,
            sourcePath.getBytes(), targetPath.getBytes());
    RenameEntryWithTempName renameEntry =
        new RenameEntryWithTempName(entry);
    List<RenameEntryWithTempName> renames =
        Lists.newArrayList(renameEntry);
    PartitionedDiffReport partitionedDiffReport =
        new PartitionedDiffReport(renames, Lists.emptyList(),
            Lists.emptyList(), Lists.emptyList(), Lists.emptyList());

    long fileLength = 43L;
    Block block = new Block(42L, fileLength, 44L);
    INodeFile iNodeFile = createINodeFile(iNodeName, block);
    LocatedBlocks locatedBlocks = createLocatedBlocks(fileLength, block);

    when(filePlannerMock.getINodeFile(syncMount, entry))
        .thenReturn(iNodeFile);
    when(filePlannerMock.getLocatedBlocks(targetSnapshotId, iNodeFile))
        .thenReturn(locatedBlocks);

    PhasedPlan phasedPlan =
        phasedPlanFactory.createFromPartitionedDiffReport(
            partitionedDiffReport, syncMount, snapshot,
            sourceSnapshotId, targetSnapshotId);

    assertThat(phasedPlan.peekCreateDirSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekCreateFileSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();
    List<SyncTask> renameToTemporaryName =
        phasedPlan.peekRenameToTemporaryName();
    assertThat(renameToTemporaryName).hasSize(1);
    List<SyncTask> renameToFinalName = phasedPlan.peekRenameToFinalName();
    assertThat(renameToFinalName).hasSize(1);

    SyncTask.RenameFileSyncTask renameToTemp =
        (SyncTask.RenameFileSyncTask) renameToTemporaryName.get(0);
    URI uri = renameToTemp.getUri();
    assertThat(uri).hasPath(remoteLocation.resolve(sourcePath).getPath());
    assertThat(renameToTemp.getRenamedTo()).isNotNull();
    String renamedToTempPath = renameToTemp.getRenamedTo().getPath();
    assertThat(renamedToTempPath).startsWith(remoteLocation.getPath());
    String tempPath =
        renamedToTempPath.substring(remoteLocation.getPath().length());

    SyncTask.RenameFileSyncTask finalRename =
        (SyncTask.RenameFileSyncTask) renameToFinalName.get(0);
    assertThat(finalRename.getOperation())
        .isEqualTo(SyncTaskOperation.RENAME_FILE);
    assertThat(finalRename.getUri()).hasPath(
        remoteLocation.resolve(tempPath).getPath());
    assertThat(finalRename.getRenamedTo()).isEqualTo(
        remoteLocation.resolve(targetPath));
  }

  @Test
  public void createFromPartitionedDiffReportEmpty()
      throws URISyntaxException {
    URI remoteLocation = new URI("remoteLocation");
    Path localPath = new Path("localPath");
    UUID syncMountId = UUID.randomUUID();
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        localPath.toString(), remoteLocation.toString(), MountMode.BACKUP);
    Optional<Integer> sourceSnapshotId = Optional.of(41);
    String snapshot = "snapshot-1";
    int targetSnapshotId = 42;
    PartitionedDiffReport partitionedDiffReport =
        new PartitionedDiffReport(Lists.emptyList(), Lists.emptyList(),
            Lists.emptyList(), Lists.emptyList(), Lists.emptyList());

    PhasedPlan phasedPlan =
        phasedPlanFactory.createFromPartitionedDiffReport(
            partitionedDiffReport, syncMount, snapshot,
            sourceSnapshotId, targetSnapshotId);

    assertThat(phasedPlan.peekCreateDirSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekCreateFileSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekRenameToFinalName()).isEmpty();
    assertThat(phasedPlan.peekRenameToTemporaryName()).isEmpty();
  }

  @Test
  public void createFromPartitionedDiffReportDelete()
      throws URISyntaxException, IOException {
    URI remoteLocation = new URI("remoteLocation/");
    Path localPath = new Path("localPath");
    UUID syncMountId = UUID.randomUUID();
    String sourcePath = "sourcePath";
    String iNodeName = localPath.toString() + "/" + sourcePath;
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        localPath.toString(), remoteLocation.toString(), MountMode.BACKUP);
    Optional<Integer> sourceSnapshotId = Optional.of(41);
    String snapshot = "snapshot-1";
    int targetSnapshotId = 42;
    DiffReportEntry entry = new DiffReportEntry(INodeType.FILE,
        DiffType.DELETE, sourcePath.getBytes());
    TranslatedEntry delete = TranslatedEntry.withNoRename(entry);
    List<TranslatedEntry> deletes = Lists.newArrayList(delete);
    PartitionedDiffReport partitionedDiffReport =
        new PartitionedDiffReport(Lists.emptyList(),
        deletes, Lists.emptyList(), Lists.emptyList(), Lists.emptyList());

    long fileLength = 43L;
    Block block = new Block(42L, fileLength, 44L);
    INodeFile iNodeFile = createINodeFile(iNodeName, block);
    LocatedBlocks locatedBlocks = createLocatedBlocks(fileLength, block);

    when(filePlannerMock.getINodeFile4Snapshot(syncMount, snapshot, entry))
        .thenReturn(iNodeFile);
    when(filePlannerMock.getLocatedBlocks(sourceSnapshotId.get(), iNodeFile))
        .thenReturn(locatedBlocks);

    PhasedPlan phasedPlan = phasedPlanFactory.createFromPartitionedDiffReport(
        partitionedDiffReport, syncMount,
        snapshot, sourceSnapshotId, targetSnapshotId);

    assertThat(phasedPlan.peekCreateDirSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekCreateFileSyncTasks()).isEmpty();
    List<SyncTask> deleteMetadataSyncTasks =
        phasedPlan.peekDeleteMetadataSyncTasks();
    assertThat(deleteMetadataSyncTasks).hasSize(1);
    SyncTask.DeleteFileSyncTask deleteFileMetadataSyncTask =
        (SyncTask.DeleteFileSyncTask) deleteMetadataSyncTasks.get(0);

    assertThat(deleteFileMetadataSyncTask.getOperation())
        .isEqualTo(SyncTaskOperation.DELETE_FILE);
    assertThat(deleteFileMetadataSyncTask.getUri()).isEqualTo(
        remoteLocation.resolve(sourcePath));
    List<Block> blocks = locatedBlocks.getLocatedBlocks().stream()
        .map(lb -> lb.getBlock().getLocalBlock())
        .collect(Collectors.toList());
    assertThat(deleteFileMetadataSyncTask.getBlocks()).isEqualTo(blocks);

    assertThat(phasedPlan.peekRenameToFinalName()).isEmpty();
    assertThat(phasedPlan.peekRenameToTemporaryName()).isEmpty();
  }

  private LocatedBlocks createLocatedBlocks(long fileLength, Block block) {
    ExtendedBlock extendedBlock = new ExtendedBlock("poolId", block);
    LocatedBlock locatedBlock = new LocatedBlock(extendedBlock, null);
    List<LocatedBlock> blocks = Lists.newArrayList(locatedBlock);
    return new LocatedBlocks(fileLength, false, blocks,
        null, true, null, null);
  }

  private INodeFile createINodeFile(String iNodeName, Block block) {
    BlockInfo blockInfo = new BlockInfoContiguous(block, (short) 0);
    ArrayList<BlockInfo> blkList = Lists.newArrayList(blockInfo);

    BlockInfo[] blockInfoArray = blkList.toArray(new BlockInfo[blkList.size()]);
    return new INodeFile(44L, iNodeName.getBytes(), PERM, 0L, 0L,
        blockInfoArray, (short) 0, 0L);
  }

  @Test
  public void createFromPartitionedDiffReportFile() throws URISyntaxException,
      IOException {
    URI remoteLocation = new URI("remoteLocation/");
    Path syncMountLocalPath = new Path("syncMountLocalPath");
    UUID syncMountId = UUID.randomUUID();
    String sourcePath = "sourcePath";

    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        syncMountLocalPath.toString(), remoteLocation.toString(),
        MountMode.BACKUP);
    Optional<Integer> sourceSnapshotId = Optional.of(41);
    long blockCollectionId = 42L;
    String snapshot = "snapshot-1";
    int targetSnapshotId = 42;
    DiffReportEntry entry = new DiffReportEntry(INodeType.FILE,
        DiffType.CREATE, sourcePath.getBytes());
    TranslatedEntry create = TranslatedEntry.withNoRename(entry);
    List<TranslatedEntry> creates = Lists.newArrayList(create);
    PartitionedDiffReport partitionedDiffReport =
        new PartitionedDiffReport(Lists.emptyList(), Lists.emptyList(),
            Lists.emptyList(), creates, Lists.emptyList());

    FileAndDirsSyncTasks createFileAndDirs = new FileAndDirsSyncTasks();
    UUID id = UUID.randomUUID();
    SyncTask fileCreate = new SyncTask.CreateFileSyncTask(id, remoteLocation,
        syncMountId.toString(), Lists.newArrayList(), blockCollectionId);

    createFileAndDirs.addFileSync(fileCreate);
    when(filePlannerMock.createPlanTreeNodeForCreatedFile(syncMount,
        targetSnapshotId, entry, create.getTranslatedName()))
        .thenReturn(fileCreate);

    PhasedPlan phasedPlan = phasedPlanFactory.createFromPartitionedDiffReport(
        partitionedDiffReport, syncMount, snapshot, sourceSnapshotId,
        targetSnapshotId);

    List<SyncTask> createMetadataSyncTasks =
        phasedPlan.peekCreateFileSyncTasks();
    assertThat(createMetadataSyncTasks).hasSize(1);
    SyncTask actual = createMetadataSyncTasks.get(0);

    assertThat(actual).isEqualTo(fileCreate);
    verify(filePlannerMock, times(1)).
        createPlanTreeNodeForCreatedFile(syncMount, targetSnapshotId, entry,
            create.getTranslatedName());
    verifyZeroInteractions(directoryPlannerMock);
  }

  @Test
  public void createFromPartitionedDiffReportDirectory()
      throws URISyntaxException, IOException {
    URI remoteLocation = new URI("remoteLocation/");
    Path syncMountLocalPath = new Path("syncMountLocalPath");
    UUID syncMountId = UUID.randomUUID();
    String sourcePath = "sourcePath";

    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        syncMountLocalPath.toString(), remoteLocation.toString(),
        MountMode.BACKUP);
    Optional<Integer> sourceSnapshotId = Optional.of(41);
    String snapshot = "snapshot-1";
    int targetSnapshotId = 42;
    DiffReportEntry entry = new DiffReportEntry(INodeType.DIRECTORY,
        DiffType.CREATE,
        sourcePath.getBytes());
    TranslatedEntry create = TranslatedEntry.withNoRename(entry);
    List<TranslatedEntry> creates = Lists.newArrayList(create);
    PartitionedDiffReport partitionedDiffReport =
        new PartitionedDiffReport(Lists.emptyList(), Lists.emptyList(),
            Lists.emptyList(), creates, Lists.emptyList());

    FileAndDirsSyncTasks createFileAndDirs = new FileAndDirsSyncTasks();
    UUID id = UUID.randomUUID();
    SyncTask dirCreate = new SyncTask.CreateDirectorySyncTask(
        id, remoteLocation, syncMountId.toString());
    createFileAndDirs.addDirSync(dirCreate);

    when(directoryPlannerMock.createPlanForDirectory(entry,
        sourcePath,
        syncMount,
        targetSnapshotId)).thenReturn(createFileAndDirs);

    PhasedPlan phasedPlan = phasedPlanFactory.createFromPartitionedDiffReport(
        partitionedDiffReport, syncMount, snapshot, sourceSnapshotId,
        targetSnapshotId);

    List<SyncTask> createMetadataSyncTasks =
        phasedPlan.peekCreateDirSyncTasks();
    assertThat(createMetadataSyncTasks).hasSize(1);
    SyncTask actual = createMetadataSyncTasks.get(0);

    assertThat(actual).isEqualTo(dirCreate);
    verify(directoryPlannerMock, times(1)).
        createPlanForDirectory(entry, sourcePath, syncMount, targetSnapshotId);
    verifyZeroInteractions(filePlannerMock);
  }

  @Test
  public void createFromPartitionedDiffReportModifications()
      throws URISyntaxException, IOException {
    URI remoteLocation = new URI("remoteLocation/");
    Path syncMountLocalPath = new Path("syncMountLocalPath");
    UUID syncMountId = UUID.randomUUID();
    String sourcePath = "sourcePath";

    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        syncMountLocalPath.toString(), remoteLocation.toString(),
        MountMode.BACKUP);
    Optional<Integer> sourceSnapshotId = Optional.of(41);
    String snapshot = "snapshot-1";
    int targetSnapshotId = 42;
    DiffReportEntry entry = new DiffReportEntry(INodeType.FILE,
        DiffType.CREATE, sourcePath.getBytes());
    TranslatedEntry modification = TranslatedEntry.withNoRename(entry);
    List<TranslatedEntry> modifies = Lists.newArrayList(modification);
    PartitionedDiffReport partitionedDiffReport =
        new PartitionedDiffReport(Lists.emptyList(),
        Lists.emptyList(), modifies, Lists.emptyList(), Lists.emptyList());

    FileAndDirsSyncTasks createFileAndDirs = new FileAndDirsSyncTasks();
    UUID id = UUID.randomUUID();
    SyncTask fileCreate = new SyncTask.ModifyFileSyncTask(id, remoteLocation,
        syncMountId.toString(), Lists.newArrayList());

    createFileAndDirs.addFileSync(fileCreate);

    when(filePlannerMock.createModifiedFileSyncTasks(targetSnapshotId,
        sourcePath.getBytes(), modification.getTranslatedName(), syncMount))
        .thenReturn(fileCreate);

    PhasedPlan phasedPlan = phasedPlanFactory.createFromPartitionedDiffReport(
        partitionedDiffReport, syncMount, snapshot, sourceSnapshotId,
        targetSnapshotId);

    List<SyncTask> modifySyncTasks = phasedPlan.peekCreateFileSyncTasks();

    assertThat(modifySyncTasks).hasSize(1);
    SyncTask metadataSyncTask = modifySyncTasks.get(0);
    assertThat(metadataSyncTask).isEqualTo(fileCreate);
    verify(filePlannerMock, times(1)).
        createModifiedFileSyncTasks(targetSnapshotId, sourcePath.getBytes(),
            modification.getTranslatedName(), syncMount);
    verifyZeroInteractions(directoryPlannerMock);
  }

  @Test
  public void testCreateFromRename() throws IOException, URISyntaxException {
    URI remoteLocation = new URI("remoteLocation/");
    Path localPath = new Path("localPath");
    UUID syncMountId = UUID.randomUUID();
    String sourcePath = "sourcePath";
    String targetPath = "targetPath";
    String iNodeName = localPath.toString() + "/" + sourcePath;
    ProvidedVolumeInfo syncMount = new ProvidedVolumeInfo(syncMountId,
        localPath.toString(), remoteLocation.toString(), MountMode.BACKUP);
    Optional<Integer> sourceSnapshotId = Optional.of(41);
    String snapshot = "snapshot-1";
    int targetSnapshotId = 42;
    DiffReportEntry entry = new DiffReportEntry(INodeType.FILE,
        DiffType.RENAME, sourcePath.getBytes(), targetPath.getBytes());
    PartitionedDiffReport partitionedDiffReport =
        new PartitionedDiffReport(Lists.emptyList(), Lists.emptyList(),
            Lists.emptyList(), Lists.emptyList(), Lists.newArrayList(entry));

    long fileLength = 43L;
    long blockCollectionId = 42L;
    Block block = new Block(42L, fileLength, 44L);
    LocatedBlocks locatedBlocks = createLocatedBlocks(fileLength, block);

    URI targetURI = remoteLocation.resolve(targetPath);
    SyncTask expected = new SyncTask.CreateFileSyncTask(
        UUID.randomUUID(),
        targetURI,
        syncMountId.toString(),
        locatedBlocks.getLocatedBlocks(),
        blockCollectionId);

    DiffReportEntry dreamtUpDiffReportInsideFunction = new DiffReportEntry(
        INodeType.FILE,
        DiffType.CREATE,
        targetPath.getBytes());

    when(filePlannerMock.createPlanTreeNodeForCreatedFile(
        syncMount,
        targetSnapshotId,
        dreamtUpDiffReportInsideFunction,
        targetPath))
        .thenReturn(expected);

    PhasedPlan phasedPlan = phasedPlanFactory.createFromPartitionedDiffReport(
        partitionedDiffReport, syncMount, snapshot,
        sourceSnapshotId, targetSnapshotId);

    assertThat(phasedPlan.peekCreateFileSyncTasks())
        .containsExactly(expected);
    assertThat(phasedPlan.peekRenameToTemporaryName()).isEmpty();
    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekRenameToFinalName()).isEmpty();
    assertThat(phasedPlan.peekCreateDirSyncTasks()).isEmpty();
  }
}
