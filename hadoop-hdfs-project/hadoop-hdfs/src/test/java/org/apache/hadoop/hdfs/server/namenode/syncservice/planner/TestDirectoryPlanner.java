package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.INodeType;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceFileFilter;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestDirectoryPlanner {

  private static final PermissionStatus PERM = PermissionStatus.createImmutable(
      "user", "group", FsPermission.createImmutable((short)0));

  @Mock
  private SyncServiceFileFilter syncServiceFilter;

  @Mock
  private FilePlanner filePlannerMock;

  @Mock
  private FSDirectory fsDirectoryMock;

  private DirectoryPlanner directoryPlanner;

  @Before
  public void setup(){
    this.directoryPlanner = new DirectoryPlanner(filePlannerMock,
        fsDirectoryMock, syncServiceFilter);
  }

  @Test
  public void createPlanForCreatedDirectory() throws Exception {
    String targetName = "not used right now so I can type whatever I want";
    String syncMountPath = "syncMountLocalPath";
    int targetSnapshotId = Snapshot.CURRENT_STATE_ID;
    String sourcePath = "sourcePath";
    long id = 43L;
    long blockCollectionId = 42L;
    String iNodeDirname = syncMountPath + "\n" + sourcePath;
    INodeDirectory iNodeDirectory =
        new INodeDirectory(id, iNodeDirname.getBytes(), PERM, 0L);

    String iNodeName = sourcePath;
    INodeFile iNodeFile = new INodeFile(44L, iNodeName.getBytes(), PERM, 0L, 0L,
        null, (short) 0, 0L);

    iNodeDirectory.addChild(iNodeFile, false, targetSnapshotId);

    URI remoteLocation = new URI("remoteMountLocation");

    String syncMountName = "fake sync mount";
    SyncMount syncMount =
        new SyncMount(syncMountName, new Path(syncMountPath), remoteLocation);
    UUID syncTaskId = UUID.randomUUID();
    SyncTask createdFile = new SyncTask.CreateFileSyncTask(syncTaskId,
        remoteLocation,
        syncMountName,
        Lists.newArrayList(),
        blockCollectionId);
    when(filePlannerMock
        .createCreatedFileSyncTasks(
            targetSnapshotId,
            iNodeFile,
            syncMount))
        .thenReturn(createdFile);

    File expectedINodePath = DirectoryPlanner.convertPathToAbsoluteFile(sourcePath.getBytes(),
        new Path(syncMountPath));
    when(fsDirectoryMock.getINode(expectedINodePath.getAbsolutePath())).thenReturn(iNodeDirectory);

    DiffReportEntry diffEntry = new DiffReportEntry(INodeType.DIRECTORY,
        DiffType.CREATE,
        sourcePath.getBytes());

    FileAndDirsSyncTasks planForCreatedDirectory = directoryPlanner
        .createPlanForDirectory(diffEntry,
            targetName,
            syncMount,
            targetSnapshotId);


    List<SyncTask> fileCreates = planForCreatedDirectory.dirTasks;
    assertThat(fileCreates).hasSize(1);
    SyncTask syncTask = fileCreates.get(0);
    assertThat(syncTask.getOperation()).isEqualTo(SyncTaskOperation.CREATE_DIRECTORY);

    Mockito.verify(filePlannerMock, times(1))
        .createCreatedFileSyncTasks(targetSnapshotId, iNodeFile, syncMount);
  }

}