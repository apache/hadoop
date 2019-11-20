package org.apache.hadoop.hdfs.server.namenode.syncservice;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.intThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestSyncServiceSatisfier {

  @Mock
  private BlockManager blockManagerMock;

  @Mock
  private Namesystem namesystemMock;

  @Mock
  private MountManager mountManagerMock;

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
  public void cancelCurrentAndScheduleFullResync() throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    String syncMountId = "mount";
    Path localPath = new Path("localPath");
    SyncMount syncMount = new SyncMount(syncMountId, localPath, new URI(""));
    when(namesystemMock.getMountManager()).thenReturn(mountManagerMock);
    when(namesystemMock.getFSDirectory()).thenReturn(fsDirectoryMock);
    when(fsDirectoryMock.getINode(any())).thenReturn(iNodeMock);
    when(iNodeMock.asDirectory()).thenReturn(iNodeDirectoryMock);
    when(iNodeMock.asFile()).thenReturn(iNodeFileMock);
    when(iNodeDirectoryMock.getSnapshot(any())).thenReturn(snapshotMock);
    when(mountManagerMock.getSyncMounts()).thenReturn(Lists.newArrayList(syncMount));

    // TODO: cleanup sourcePath and .sourcePath.crc files that are generated
    // by this test.
    DiffReportEntry entry = new DiffReportEntry(SnapshotDiffReport.INodeType.FILE, SnapshotDiffReport.DiffType.CREATE,
        "sourcePath".getBytes());
    SnapshotDiffReport diffReport = new SnapshotDiffReport("snapshotRoot",
        "fromSnapshot",
        "toSnapshot",
        Lists.newArrayList(entry));
    when(mountManagerMock.makeSnapshotAndPerformDiff(localPath)).thenReturn(diffReport);

    SyncServiceSatisfier syncServiceSatisfier =
        new SyncServiceSatisfier(namesystemMock, blockManagerMock, conf);
    syncServiceSatisfier.start(false);

    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();
    syncServiceSatisfier.scheduleOnce();

    boolean cancelled = syncServiceSatisfier.cancelCurrentAndScheduleFullResync(syncMountId);

    assertThat(cancelled).isTrue();
  }
}