package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIntegrationPhasedSyncMountSnapshotUpdateFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIntegrationPhasedSyncMountSnapshotUpdateFactory.class);
  private static final long SEED = 0;
  private static final short REPLICATION = 1;
  private static final short REPLICATION_1 = 2;
  private static final long BLOCKSIZE = 1024;

  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem hdfs;
  private Comparator<SyncTask> compareNoBlocks;
  private Comparator<SyncTask> compareToTempNoBlocks;
  private Comparator<SyncTask> compareFromTempNoBlocks;

  @Before
  public void setUp() throws Exception {
    compareNoBlocks = (o1, o2) -> {
      int uriCompare = o1.getUri().compareTo(o2.getUri());
      if(uriCompare == 0){
        int syncMountEqual = o1.getSyncMountId().compareTo(o2.getSyncMountId());
        if(syncMountEqual == 0){
          return o1.getOperation().compareTo(o2.getOperation());
        } else {
          return syncMountEqual;
        }
      } else {
        return uriCompare;
      }
    };
    compareToTempNoBlocks = (o1, o2) ->{
      int uriCompare = o1.getUri().compareTo(o2.getUri());
      if(uriCompare == 0){
        int syncMountEqual = o1.getSyncMountId().compareTo(o2.getSyncMountId());
        if(syncMountEqual == 0){
          int opsCompare = o1.getOperation().compareTo(o2.getOperation());
          if(opsCompare == 0){
            if(o1.getOperation().equals(MetadataSyncTaskOperation.RENAME_FILE)) {
              SyncTask.RenameFileSyncTask rename1 =
                  (SyncTask.RenameFileSyncTask) o1;
              SyncTask.RenameFileSyncTask rename2 =
                  (SyncTask.RenameFileSyncTask) o2;
              boolean tmp1 = new Path(rename1.renamedTo.getPath()).getName().startsWith("tmp");
              boolean tmp2 = new Path(rename2.renamedTo.getPath()).getName().startsWith("tmp");
              if (tmp1 && tmp2) {
                return 0;
              } else {
                return -1;
              }
            }
            if(o1.getOperation().equals(MetadataSyncTaskOperation.RENAME_DIRECTORY)) {
              SyncTask.RenameDirectorySyncTask rename1 =
                  (SyncTask.RenameDirectorySyncTask) o1;
              SyncTask.RenameDirectorySyncTask rename2 =
                  (SyncTask.RenameDirectorySyncTask) o2;
              boolean tmp1 = new Path(rename1.renamedTo.getPath()).getName().startsWith("tmp");
              boolean tmp2 = new Path(rename2.renamedTo.getPath()).getName().startsWith("tmp");
              if (tmp1 && tmp2) {
                return 0;
              } else {
                return -1;
              }
            }
            return 0;
          } else {
            return opsCompare;
          }
        } else {
          return syncMountEqual;
        }
      } else {
        return uriCompare;
      }
    };
    compareFromTempNoBlocks = (o1, o2) ->{
      boolean tmp1 = new Path(o1.getUri().getPath()).getName().startsWith("tmp");
      boolean tmp2 = new Path(o2.getUri().getPath()).getName().startsWith("tmp");
      if(tmp1 && tmp2){
        int syncMountEqual = o1.getSyncMountId().compareTo(o2.getSyncMountId());
        if(syncMountEqual == 0){
          int opsCompare = o1.getOperation().compareTo(o2.getOperation());
          if(opsCompare == 0){
            if(o1.getOperation().equals(MetadataSyncTaskOperation.RENAME_FILE)) {
              SyncTask.RenameFileSyncTask rename1 =
                  (SyncTask.RenameFileSyncTask) o1;
              SyncTask.RenameFileSyncTask rename2 =
                  (SyncTask.RenameFileSyncTask) o2;
              return rename1.renamedTo.compareTo(rename2.renamedTo);
            }
            if(o1.getOperation().equals(MetadataSyncTaskOperation.RENAME_DIRECTORY)) {
              SyncTask.RenameDirectorySyncTask rename1 =
                  (SyncTask.RenameDirectorySyncTask) o1;
              SyncTask.RenameDirectorySyncTask rename2 =
                  (SyncTask.RenameDirectorySyncTask) o2;
              return rename1.renamedTo.compareTo(rename2.renamedTo);
            }
            return 0;

          } else {
            return opsCompare;
          }
        } else {
          return syncMountEqual;
        }
      } else {
        return -1;
      }
    };
    conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE,
        true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT,
        true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT, 3);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(true).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testSimpleCreateFile() throws Exception {
    final Path dir = new Path("/testSimpleCreateFile");
    final Path sub1 = new Path(dir, "sub1");

    //given
    hdfs.mkdirs(dir);
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    URI remoteLocation = new URI("hdfs://host/path/");
    Path file10 = new Path(dir, "file10");
    Path file11 = new Path(dir, "file11");
    Path file12 = new Path(dir, "file12");
    Path file13 = new Path(dir, "file13");
    DFSTestUtil.createFile(hdfs, file10, BLOCKSIZE, REPLICATION_1, SEED);
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, REPLICATION_1, SEED);
    DFSTestUtil.createFile(hdfs, file12, BLOCKSIZE, REPLICATION_1, SEED);
    DFSTestUtil.createFile(hdfs, file13, BLOCKSIZE, REPLICATION_1, SEED);


    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest = new
        PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
        cluster.getNamesystem().getBlockManager(), new Configuration());


    PhasedPlan planFromDiffReport = underTest.createPlanFromDiffReport(syncMount,
        snapshotDiffReport, Optional.of( -1), -1);
    //then
    assertThat(planFromDiffReport).isNotNull();

    List<SyncTask> createFileSyncTasks =
        planFromDiffReport.peekCreateFileSyncTasks();


    URI baseURI = new URI(dir.toString());
    assertThat(createFileSyncTasks)
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createFile(remoteLocation, file10, syncMount),
            createFile(remoteLocation, file11, syncMount),
            createFile(remoteLocation, file12, syncMount),
            createFile(remoteLocation, file13, syncMount)
        );


    assertThat(planFromDiffReport.peekRenameToTemporaryName()).isEmpty();
    assertThat(planFromDiffReport.peekCreateDirSyncTasks()).isEmpty();
    assertThat(planFromDiffReport.peekRenameToFinalName()).isEmpty();
    assertThat(planFromDiffReport.peekDeleteMetadataSyncTasks()).isEmpty();
  }



  /**
   * #given /basic-test
   * mkdir -p /basic-test/a/b/c
   * touch /basic-test/a/b/c/d/f1.bin
   * touch /basic-test/f1.bin
   */
  @Test(timeout = 60000)
  public void testNewDirsNewFiles() throws Exception {
    final Path dir = new Path("/testNewDirsNewFiles");
    final Path subDir = new Path(dir, "a/b/c");
    final Path file01SubDir = new Path(subDir, "file01");
    final Path file01 = new Path(dir, "file01");

    hdfs.mkdirs(dir);
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);
    DFSTestUtil.createFile(hdfs, file01SubDir, BLOCKSIZE, REPLICATION_1, SEED);
    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest = new
        PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
        cluster.getNamesystem().getBlockManager(), new Configuration());
    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport,
            Optional.of(-1), -1);

    assertThat(phasedPlan.peekCreateDirSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createDirectory(remoteLocation, new Path("a"), syncMount),
            createDirectory(remoteLocation, new Path("a/b") , syncMount),
            createDirectory(remoteLocation, new Path("a/b/c"), syncMount)
        );

    URI baseURI = new URI(dir.toString());
    assertThat(phasedPlan.peekCreateFileSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createFile(remoteLocation, file01SubDir, syncMount),
            createFile(remoteLocation, file01, syncMount)
            );

    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekRenameToFinalName()).isEmpty();
    assertThat(phasedPlan.peekRenameToTemporaryName()).isEmpty();
  }

  /**
   * given /basic-test; /basic-test/f1.bin
   * mkdir -p /basic-test/a/b/c
   * touch /basic-test/a/b/c/d/f1.bin
   * mv /basic-test/f1.bin /basic-test/a
   */
  @Test(timeout = 60000)
  public void testNewDirsMoveFiles() throws Exception {
    final Path dir = new Path("/testNewDirsMoveFiles");
    final Path subDir = new Path(dir, "a/b/c");
    final Path file01SubDir = new Path(subDir, "file01");
    final Path file01 = new Path(dir, "file01");

    hdfs.mkdirs(dir);
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    DFSTestUtil.createFile(hdfs, file01SubDir, BLOCKSIZE, REPLICATION_1, SEED);
    Path renamedFile01 = new Path(dir, "a/file01");
    hdfs.rename(file01, renamedFile01);
    hdfs.createSnapshot(dir, "s2");


    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    URI baseURI = new URI(dir.toString());

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest =
        new PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
            cluster.getNamesystem().getBlockManager(), new Configuration());

    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport,
            Optional.of(42), 43);

    assertThat(phasedPlan.peekRenameToTemporaryName())
        .usingElementComparator(compareToTempNoBlocks)
        .containsExactly(
            renameToTempFile(remoteLocation, file01, syncMount
            ));

    assertThat(phasedPlan.peekCreateDirSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createDirectory(remoteLocation, new Path("a"), syncMount),
            createDirectory(remoteLocation, new Path("a/b"), syncMount),
            createDirectory(remoteLocation, new Path("a/b/c"), syncMount)
        );

    assertThat(phasedPlan.peekCreateFileSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createFile(remoteLocation, file01SubDir, syncMount),
            createFile(remoteLocation, renamedFile01, syncMount)

        );

    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();

    assertThat(phasedPlan.peekRenameToFinalName())
        .usingElementComparator(compareFromTempNoBlocks)
        .containsExactly(
            renameFromTempFile(remoteLocation, renamedFile01, syncMount)
        );


//    verifyPlan(syncMountSnapshotUpdatePlan,
//        modifyDirectory()
//            .andThen(createDirectory(subDir))
//            .andThen(renameFile(file01, file01SubDir))
//            .andThen(createFile(file01)));
  }

  @Test(timeout = 60000)
  public void testModifyDirectory() throws Exception {
    final Path dir = new Path("/testModifyDirectory");
    final Path subDir = new Path(dir, "a/b/c");
    final Path file01SubDir = new Path(subDir, "file01");

    hdfs.mkdirs(dir);
    DFSTestUtil.createFile(hdfs, file01SubDir, BLOCKSIZE, REPLICATION_1, SEED);
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    hdfs.setStoragePolicy(subDir, "HOT");
    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest =
        new PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
            cluster.getNamesystem().getBlockManager(), new Configuration());
    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport,
            Optional.of(-1), -1);
    //then
    assertThat(phasedPlan.peekRenameToTemporaryName()).isEmpty();
    assertThat(phasedPlan.peekRenameToFinalName()).isEmpty();
    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekCreateDirSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekCreateFileSyncTasks()).isEmpty();
  }

  @Test(timeout = 60000)
  public void testSwapFiles() throws Exception {
    final Path dir = new Path("/testSwapFiles");
    final Path file01= new Path(dir, "file01");
    final Path file02 = new Path(dir, "file02");
    final Path tmp = new Path(dir, "tmp");

    hdfs.mkdirs(dir);
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);
    DFSTestUtil.createFile(hdfs, file02, BLOCKSIZE, REPLICATION_1, SEED);
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    hdfs.rename(file01, tmp);
    hdfs.rename(file02, file01);
    hdfs.rename(tmp, file02);

    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest =
        new PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
            cluster.getNamesystem().getBlockManager(), new Configuration());
    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport,
            Optional.of(42), 43);

    assertThat(phasedPlan.peekRenameToTemporaryName())
        .usingElementComparator(compareToTempNoBlocks)
        .containsExactly(
            renameToTempFile(remoteLocation, file02, syncMount),
            renameToTempFile(remoteLocation, file01, syncMount)
        );

    assertThat(phasedPlan.peekRenameToFinalName())
        .usingElementComparator(compareFromTempNoBlocks)
        .containsExactly(
            renameFromTempFile(remoteLocation, file02, syncMount),
            renameFromTempFile(remoteLocation, file01, syncMount)
        );

    assertThat(phasedPlan.peekCreateFileSyncTasks()).isEmpty();

    assertThat(phasedPlan.peekCreateDirSyncTasks()).isEmpty();

    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();

  }

  @Test(timeout = 60000)
  public void testSwapDirectories() throws Exception {
    final Path dir = new Path("/testSwapDirectories");
    final Path subDir01 = new Path(dir, "subDir01");
    final Path subDir02 = new Path(dir, "subDir02");
    final Path tmp = new Path(dir, "tmp");

    hdfs.mkdirs(dir);
    hdfs.mkdirs(subDir01);
    hdfs.mkdirs(subDir02);
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    hdfs.rename(subDir01, tmp);
    hdfs.rename(subDir02, subDir01);
    hdfs.rename(tmp, subDir02);

    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest =
        new PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
            cluster.getNamesystem().getBlockManager(), new Configuration());
    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport,
            Optional.of(-1), -1);

    assertThat(phasedPlan.peekRenameToTemporaryName())
        .usingElementComparator(compareToTempNoBlocks)
        .containsExactly(
            renameToTempDirectory(remoteLocation,subDir02, syncMount),
            renameToTempDirectory(remoteLocation,subDir01, syncMount)
        );

    assertThat(phasedPlan.peekRenameToFinalName())
        .usingElementComparator(compareFromTempNoBlocks)
        .containsExactly(
            renameFromTempDirectory(remoteLocation,subDir02, syncMount),
            renameFromTempDirectory(remoteLocation, subDir01, syncMount)
        );


    assertThat(phasedPlan.peekCreateFileSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekCreateDirSyncTasks()).isEmpty();
    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();
  }

  @Test(timeout = 60000)
  public void testNewFileInOldDirectoryName() throws Exception {
    final Path dir = new Path("/testNewFileInOldDirectoryName");
    final Path dirE = new Path(dir, "a/b/c/d/e");
    final Path dirEBackup = new Path(dir, "a/b/c/d/e.backup");
    final Path file01 = new Path(dirE, "file01");

    //given
    hdfs.mkdirs(dir);
    hdfs.mkdirs(dirE);

    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    hdfs.rename(dirE, dirEBackup);
    hdfs.mkdirs(dirE);
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);

    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest =
        new PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
            cluster.getNamesystem().getBlockManager(), new Configuration());
    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport,
            Optional.of(-1), -1);


    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();

    assertThat(phasedPlan.peekRenameToTemporaryName())
        .usingElementComparator(compareToTempNoBlocks)
        .containsExactly(
            renameToTempDirectory(remoteLocation, dirE, syncMount)
        );

    assertThat(phasedPlan.peekCreateFileSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createFile(remoteLocation,file01, syncMount)
        );

    assertThat(phasedPlan.peekRenameToFinalName())
        .usingElementComparator(compareFromTempNoBlocks)
        .containsExactly(
            renameFromTempDirectory(remoteLocation, dirEBackup, syncMount)
        );

    assertThat(phasedPlan.peekCreateDirSyncTasks())
        .usingElementComparator(compareToTempNoBlocks)
        .containsExactly(createDirectory(remoteLocation, dirE, syncMount)
        );

  }

  @Test(timeout = 60000)
  public void testMoveDirCreateNewDirInPlace() throws Exception {
    final Path dir = new Path("/testMoveDirCreateDirMoveFiles");
    final Path dirB = new Path(dir, "a/b");
    final Path dirBB = new Path(dir, "a/bb");
    final Path file01 = new Path(dir, "file01");
    final Path file01B = new Path(dirB, "file01");
    final Path file01BB = new Path(dirBB, "file01");

    //given
    hdfs.mkdirs(dir);
    hdfs.mkdirs(dirB);
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);
    DFSTestUtil.createFile(hdfs, file01B, BLOCKSIZE, REPLICATION_1, SEED);

    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    hdfs.rename(dirB, dirBB);
    hdfs.mkdirs(dirB);
    hdfs.rename(file01BB, file01B);
    hdfs.rename(file01, file01BB);
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);

    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest =
        new PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
            cluster.getNamesystem().getBlockManager(), new Configuration());
    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport,
            Optional.of(42), 43);

    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();

    assertThat(phasedPlan.peekRenameToTemporaryName())
        .usingElementComparator(compareToTempNoBlocks)
        .containsExactly(
            renameToTempFile(remoteLocation,file01, syncMount),
            renameToTempFile(remoteLocation, file01B, syncMount),
            renameToTempDirectory(remoteLocation, dirB, syncMount)
        );

    assertThat(phasedPlan.peekCreateFileSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createFile(remoteLocation, file01,syncMount),
            createFile(remoteLocation, file01B, syncMount));

    assertThat(phasedPlan.peekRenameToFinalName())
        .usingElementComparator(compareFromTempNoBlocks)
        .containsExactly(
            renameFromTempDirectory(remoteLocation, dirBB, syncMount),
            renameFromTempFile(remoteLocation, file01B, syncMount),
            renameFromTempFile(remoteLocation, file01BB, syncMount)
        );


    assertThat(phasedPlan.peekCreateDirSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createDirectory(remoteLocation, dirB, syncMount)
        );

    //then
//    assertNotNull(syncMountSnapshotUpdatePlan);
//    verifyPlan(syncMountSnapshotUpdatePlan,
//        modifyDirectory()
//            .andThen(renameDirectory(dirB, dirBB))
//            .andThen(createDirectory(dirB))
//            .andThen(renameFile(file01BB, file01B))
//            .andThen(renameFile(file01, file01BB)));
  }

  @Test(timeout = 60000)
  public void testMoveDirCreateDirMoveFiles() throws Exception {
    final Path dir = new Path("/testMoveDirCreateDirMoveFiles");
    final Path dirB = new Path(dir, "a/b");
    final Path dirBB = new Path(dir, "a/bb");
    final Path file01 = new Path(dir, "file01");
    final Path file01B = new Path(dirB, "file01");
    final Path file01BB = new Path(dirBB, "file01");

    //given
    hdfs.mkdirs(dir);
    hdfs.mkdirs(dirB);
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);
    DFSTestUtil.createFile(hdfs, file01B, BLOCKSIZE, REPLICATION_1, SEED);

    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s1");

    hdfs.rename(dirB, dirBB);
    hdfs.mkdirs(dirB);
    hdfs.rename(file01BB, file01B);
    hdfs.rename(file01, file01BB);
    DFSTestUtil.createFile(hdfs, file01, BLOCKSIZE, REPLICATION_1, SEED);

    hdfs.createSnapshot(dir, "s2");
    SnapshotDiffReport snapshotDiffReport= hdfs.getSnapshotDiffReport(dir, "s1", "s2");
    URI remoteLocation = new URI("hdfs://host/path/");
    SyncMount syncMount = new SyncMount("StubbedSyncMount", dir, remoteLocation);

    //when
    PhasedSyncMountSnapshotUpdateFactory underTest = new
        PhasedSyncMountSnapshotUpdateFactory(cluster.getNamesystem(),
        cluster.getNamesystem().getBlockManager(), new Configuration());
    PhasedPlan phasedPlan =
        underTest.createPlanFromDiffReport(syncMount, snapshotDiffReport, Optional.of(42), 43);

    assertThat(phasedPlan.peekDeleteMetadataSyncTasks()).isEmpty();

    assertThat(phasedPlan.peekRenameToTemporaryName())
        .usingElementComparator(compareToTempNoBlocks)
        .containsExactly(
            renameToTempFile(remoteLocation, file01, syncMount),
            renameToTempFile(remoteLocation, file01B, syncMount),
            renameToTempDirectory(remoteLocation, dirB, syncMount)
        );

    assertThat(phasedPlan.peekCreateFileSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createFile(remoteLocation, file01, syncMount),
            createFile(remoteLocation, file01B, syncMount)
            );

    assertThat(phasedPlan.peekRenameToFinalName())
        .usingElementComparator(compareFromTempNoBlocks)
        .containsExactly(
            renameFromTempDirectory(remoteLocation, dirBB, syncMount),
            renameFromTempFile(remoteLocation, file01B, syncMount),
            renameFromTempFile(remoteLocation, file01BB, syncMount)
        );

    assertThat(phasedPlan.peekCreateDirSyncTasks())
        .usingElementComparator(compareNoBlocks)
        .containsExactly(
            createDirectory(remoteLocation, dirB, syncMount)
        );

    //then
//    assertNotNull(syncMountSnapshotUpdatePlan);
//    verifyPlan(syncMountSnapshotUpdatePlan,
//        modifyDirectory()
//            .andThen(renameDirectory(dirB, dirBB))
//            .andThen(createDirectory(dirB))
//            .andThen(renameFile(file01BB, file01B))
//            .andThen(renameFile(file01, file01BB)));
  }

  private SyncTask createFile(URI remote, Path path, SyncMount syncMount) throws URISyntaxException {
    Path localPath = syncMount.getLocalPath();
    URI baseURI = new URI(localPath.toString());
    String relativizedPath = baseURI.relativize(path.toUri()).getPath();
    long blockCollectionId = 42L;
    return SyncTask.createFile(
        remote.resolve(relativizedPath),
        syncMount.getName(),
        Collections.emptyList(),
        blockCollectionId);
  }

  private SyncTask renameToTempFile(URI remote, Path from, SyncMount syncMount) throws URISyntaxException {
    Path localPath = syncMount.getLocalPath();
    URI baseURI = new URI(localPath.toString());
    String relativizedPath = baseURI.relativize(from.toUri()).getPath();
    return SyncTask.renameFile(remote.resolve(relativizedPath),
        remote.resolve("tmp"), Collections.emptyList(), syncMount.getName());
  }

  private SyncTask renameFromTempFile(URI remote, Path to, SyncMount syncMount) throws URISyntaxException {
    Path localPath = syncMount.getLocalPath();
    URI baseURI = new URI(localPath.toString());
    String relativizedPath = baseURI.relativize(to.toUri()).getPath();
    return SyncTask.renameFile(remote.resolve("tmp"),
        remote.resolve(relativizedPath), Collections.emptyList(), syncMount.getName());
  }


  private SyncTask renameToTempDirectory(URI base, Path from, SyncMount syncMount) throws URISyntaxException {
    Path localPath = syncMount.getLocalPath();
    URI baseURI = new URI(localPath.toString());
    String relativizedPath = baseURI.relativize(from.toUri()).getPath();
    return SyncTask.renameDirectory(base.resolve(relativizedPath),
        base.resolve("tmp"), syncMount.getName());
  }

  private SyncTask renameFromTempDirectory(URI base, Path to, SyncMount syncMount) throws URISyntaxException {
    Path localPath = syncMount.getLocalPath();
    URI baseURI = new URI(localPath.toString());
    String relativizedPath = baseURI.relativize(to.toUri()).getPath();
    return SyncTask.renameDirectory(base.resolve("tmp"),
        base.resolve(relativizedPath), syncMount.getName());
  }

  private SyncTask createDirectory(URI remote, Path path, SyncMount syncMount) throws URISyntaxException {
    Path localPath = syncMount.getLocalPath();
    URI baseURI = new URI(localPath.toString());
    String relativizedPath = baseURI.relativize(path.toUri()).getPath();
    return SyncTask.createDirectory(remote.resolve(relativizedPath),
        syncMount.getName());

  }

}