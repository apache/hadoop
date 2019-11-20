package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestFilePlanner {

  private static final PermissionStatus PERM = PermissionStatus.createImmutable(
      "user", "group", FsPermission.createImmutable((short)0));

  private FilePlanner filePlanner;

  @Mock
  private Namesystem namesystemMock;

  @Mock
  private BlockManager blockManagerMock;


  @Before
  public void setup(){
    filePlanner = new FilePlanner(namesystemMock, blockManagerMock);
  }

  @Test
  public void createFileWithZeroSize() throws Exception {
    int targetSnapshotId = 42;
    String syncMountLocalPath = "localPath/";
    String sourcePath = "sourcePath";
    String iNodeName = syncMountLocalPath + sourcePath;
    URI remoteLocation = new URI("remoteLocation/");

    String syncMountId = "mock";
    SyncMount syncMount =
        new SyncMount(syncMountId, new Path(syncMountLocalPath), remoteLocation);


    INodeFile iNodeFile = new INodeFile(44L, iNodeName.getBytes(), PERM, 0L, 0L,
        null, (short) 0, 0L);

    SyncTask createdFileSyncTasks = filePlanner
        .createCreatedFileSyncTasks(targetSnapshotId, iNodeFile, syncMount);

    assertThat(createdFileSyncTasks).isNotNull();
    assertThat(createdFileSyncTasks.getOperation()).isEqualTo(SyncTaskOperation.TOUCH_FILE);
    assertThat(createdFileSyncTasks.getSyncMountId()).isEqualTo(syncMountId);
    assertThat(createdFileSyncTasks.getUri()).isEqualTo(remoteLocation.resolve(sourcePath));
  }

  @Test
  public void createFileSingleBlock() throws Exception {
    int targetSnapshotId = 42;
    String syncMountLocalPath = "localPath/";
    String sourcePath = "sourcePath";
    String iNodeName = syncMountLocalPath + sourcePath;
    URI remoteLocation = new URI("remoteLocation/");

    String syncMountId = "mock";
    SyncMount syncMount =
        new SyncMount(syncMountId, new Path(syncMountLocalPath), remoteLocation);

    long fileLength = 43L;

    Block block = new Block(42L, fileLength, 44L);
    BlockInfo blockInfo = new BlockInfoContiguous(block, (short)0);

    ArrayList<BlockInfo> blkList = Lists.newArrayList(blockInfo);

    BlockInfo[] blockInfoArray = blkList.toArray(new BlockInfo[blkList.size()]);
    INodeFile iNodeFile = new INodeFile(44L, iNodeName.getBytes(), PERM, 0L, 0L,
        blockInfoArray, (short) 0, 0L);


    ExtendedBlock extendedBlock = new ExtendedBlock("poolId", block);
    LocatedBlock locatedBlock = new LocatedBlock(extendedBlock, null);
    List<LocatedBlock> blocks = Lists.newArrayList(locatedBlock);
    LocatedBlocks locatedBlocks = new LocatedBlocks(fileLength, false,
        blocks, null, true, null, null);
    when(blockManagerMock
        .createLocatedBlocks(blockInfoArray, fileLength,
            false, 0, fileLength,
            false, true, null, null))
        .thenReturn(locatedBlocks);

    SyncTask createdFileSyncTasks = filePlanner
        .createCreatedFileSyncTasks(targetSnapshotId, iNodeFile, syncMount);

    assertThat(createdFileSyncTasks).isNotNull();
    assertThat(createdFileSyncTasks.getOperation()).isEqualTo(SyncTaskOperation.CREATE_FILE);
    assertThat(createdFileSyncTasks.getSyncMountId()).isEqualTo(syncMountId);
    assertThat(createdFileSyncTasks.getUri()).isEqualTo(remoteLocation.resolve(sourcePath));

  }

}