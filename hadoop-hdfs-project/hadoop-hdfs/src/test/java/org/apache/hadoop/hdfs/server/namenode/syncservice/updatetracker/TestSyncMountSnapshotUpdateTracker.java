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
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.CREATE_DIRS;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.CREATE_FILES;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.DELETES;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.FINISHED;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.NOT_STARTED;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.RENAMES_TO_FINAL;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.RENAMES_TO_TEMP;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.phases.MultipartPhase.COMPLETE_PHASE;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.phases.MultipartPhase.INIT_PHASE;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.phases.MultipartPhase.PUT_PHASE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test SyncMountSnapshotUpdateTrackerImpl.
 */
public class TestSyncMountSnapshotUpdateTracker {

  private String syncMountId = "MySyncMount";
  private Configuration configuration = new Configuration();

  private BlockAliasMap.Writer<FileRegion> dummyAliasMapWriter =
      new BlockAliasMap.Writer<FileRegion>() {
        @Override
        public void store(FileRegion blockAlias) throws IOException {
        }

        @Override
        public void remove(Block block) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
      };

  @Test
  public void testSingleSyncTask() throws URISyntaxException {

    //given:
    URI uri = new URI("test://host/path");
    URI toUri = new URI("test://host/frog");
    SyncTask syncTask =
        SyncTask.renameFile(uri, toUri, Collections.emptyList(),
        syncMountId);
    MetadataSyncTask metadataSyncTask1 = MetadataSyncTask.
        renameFile((SyncTask.RenameFileSyncTask) syncTask);
    //when:
    PhasedPlan phasedPlan = new PhasedPlan(
        Lists.newArrayList(syncTask),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    SyncMountSnapshotUpdateTrackerImpl underTest =
        new SyncMountSnapshotUpdateTrackerImpl(phasedPlan, dummyAliasMapWriter,
            configuration);
    assertThat(underTest.getCurrentPhase()).isEqualTo(NOT_STARTED);
    SchedulableSyncPhase renameToTempPhase =
        underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renameToTempPhase).isEqualTo(
        SchedulableSyncPhase.createMeta(Lists.newArrayList(metadataSyncTask1)
    ));

    markPhaseSuccessful(renameToTempPhase, underTest);

    SchedulableSyncPhase deletePhase = underTest.getNextSchedulablePhase();
    assertThat(deletePhase)
        .isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(DELETES);

    markPhaseSuccessful(deletePhase, underTest);

    SchedulableSyncPhase renamesToFinal = underTest.getNextSchedulablePhase();
    assertThat(renamesToFinal)
        .isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_FINAL);

    markPhaseSuccessful(renamesToFinal, underTest);

    SchedulableSyncPhase createDirsPhase = underTest.getNextSchedulablePhase();
    assertThat(createDirsPhase)
        .isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_DIRS);

    markPhaseSuccessful(createDirsPhase, underTest);

    SchedulableSyncPhase createFilesPhase = underTest.getNextSchedulablePhase();
    assertThat(createFilesPhase)
        .isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(createFilesPhase, underTest);

    SchedulableSyncPhase finishedPhase = underTest.getNextSchedulablePhase();
    assertThat(finishedPhase)
        .isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(FINISHED);

    assertThat(underTest.isFinished()).isTrue();
    assertThat(underTest.getNextSchedulablePhase())
        .isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getNextSchedulablePhase())
        .isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getNextSchedulablePhase())
        .isEqualTo(SchedulableSyncPhase.empty());
  }

  @Test
  public void testAllSyncTasks() throws URISyntaxException {

    URI uri = new URI("test://host/path");
    URI renamedTo = new URI("test://host/frog");
    SyncTask createDirectory = SyncTask.createDirectory(uri, syncMountId);
    SyncTask createFile = createFileSyncTask(uri, 1024, 1, 1);
    SyncTask deleteFile =
        SyncTask.deleteFile(uri, Collections.emptyList(), syncMountId);
    SyncTask deleteDirectory = SyncTask.deleteDirectory(uri, syncMountId);
    SyncTask renameFile = SyncTask.renameFile(uri, renamedTo,
        Collections.emptyList(), syncMountId);
    SyncTask renameDirectory =
        SyncTask.renameDirectory(uri, renamedTo, syncMountId);

    MetadataSyncTask expectedCreateDirectory = MetadataSyncTask.createDirectory(
        (SyncTask.CreateDirectorySyncTask) createDirectory
    );
    MetadataSyncTask expectedDeleteFile = MetadataSyncTask.deleteFile(
        (SyncTask.DeleteFileSyncTask) deleteFile
    );
    MetadataSyncTask expectedDeleteDirectory = MetadataSyncTask.deleteDirectory(
        (SyncTask.DeleteDirectorySyncTask) deleteDirectory
    );
    MetadataSyncTask expectedRenameFile = MetadataSyncTask.renameFile(
        (SyncTask.RenameFileSyncTask) renameFile
    );
    MetadataSyncTask expectedRenameDirectory =
        MetadataSyncTask.renameDirectory(
            (SyncTask.RenameDirectorySyncTask) renameDirectory
    );

    PhasedPlan phasedPlan = new PhasedPlan(
        Lists.newArrayList(renameFile),
        Lists.newArrayList(deleteDirectory, deleteFile),
        Lists.newArrayList(renameDirectory),
        Lists.newArrayList(createDirectory),
        Lists.newArrayList(createFile)
    );

    SyncMountSnapshotUpdateTrackerImpl underTest =
        new SyncMountSnapshotUpdateTrackerImpl(phasedPlan, dummyAliasMapWriter,
            configuration);
    assertThat(underTest.getCurrentPhase()).isEqualTo(NOT_STARTED);
    SchedulableSyncPhase renamesToTempPhase =
        underTest.getNextSchedulablePhase();

    //then:
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renamesToTempPhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameFile)
    ));

    markPhaseSuccessful(renamesToTempPhase, underTest);

    SchedulableSyncPhase deletePhase = underTest.getNextSchedulablePhase();
    assertThat(deletePhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedDeleteDirectory, expectedDeleteFile)
    ));
    assertThat(underTest.getCurrentPhase()).isEqualTo(DELETES);

    markPhaseSuccessful(deletePhase, underTest);

    SchedulableSyncPhase renamePhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_FINAL);
    assertThat(renamePhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameDirectory)
    ));

    markPhaseSuccessful(renamePhase, underTest);

    SchedulableSyncPhase createDirsPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_DIRS);
    assertThat(createDirsPhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedCreateDirectory)
    ));

    markPhaseSuccessful(createDirsPhase, underTest);

    // Multipart Init
    SchedulableSyncPhase mpInitPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(INIT_PHASE);

    markPhaseSuccessful(mpInitPhase, underTest);

    // Multipart putPart
    SchedulableSyncPhase mpPutPartPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(PUT_PHASE);
    markPhaseSuccessful(mpPutPartPhase, underTest);

    // Multipart Complete
    SchedulableSyncPhase mpCompletePhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(COMPLETE_PHASE);

    markPhaseSuccessful(mpCompletePhase, underTest);

    // Complete
    SchedulableSyncPhase finalPhase = underTest.getNextSchedulablePhase();
    assertThat(finalPhase).isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(FINISHED);
    markPhaseSuccessful(finalPhase, underTest);
  }

  @Test
  public void testTooManyFailures() throws URISyntaxException {

    long blockCollectionId = 42L;
    URI uri = new URI("test://host/path");
    URI renamedTo = new URI("test://host/frog");
    SyncTask createDirectory = SyncTask.createDirectory(uri, syncMountId);
    SyncTask createFile = SyncTask.createFile(uri, syncMountId,
        Collections.emptyList(), blockCollectionId);
    SyncTask deleteFile =
        SyncTask.deleteFile(uri, Collections.emptyList(), syncMountId);
    SyncTask deleteDirectory = SyncTask.deleteDirectory(uri, syncMountId);
    SyncTask renameFile = SyncTask.renameFile(uri, renamedTo,
        Collections.emptyList(), syncMountId);
    SyncTask renameDirectory =
        SyncTask.renameDirectory(uri, renamedTo, syncMountId);

    MetadataSyncTask expectedRenameFile = MetadataSyncTask.renameFile(
        (SyncTask.RenameFileSyncTask) renameFile
    );

    PhasedPlan phasedPlan = new PhasedPlan(
        Lists.newArrayList(renameFile),
        Lists.newArrayList(deleteDirectory, deleteFile),
        Lists.newArrayList(renameDirectory),
        Lists.newArrayList(createDirectory),
        Lists.newArrayList(createFile)
    );

    SyncMountSnapshotUpdateTrackerImpl underTest =
        new SyncMountSnapshotUpdateTrackerImpl(phasedPlan, dummyAliasMapWriter,
            configuration);
    assertThat(underTest.getCurrentPhase()).isEqualTo(NOT_STARTED);

    SchedulableSyncPhase renamesPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renamesPhase).isEqualTo(SchedulableSyncPhase.createMeta(
         Lists.newArrayList(expectedRenameFile))
    );

    markPhaseFailed(renamesPhase, underTest);

    SchedulableSyncPhase renamesRetryPhase1 =
        underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renamesRetryPhase1).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameFile))
    );

    markPhaseFailed(renamesRetryPhase1, underTest);

    SchedulableSyncPhase renamesRetryPhase2 =
        underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renamesRetryPhase2).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameFile))
    );

    markPhaseFailed(renamesRetryPhase2, underTest);

    SchedulableSyncPhase renamesRetryPhase3 =
        underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renamesRetryPhase3).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameFile))
    );

    markPhaseFailed(renamesRetryPhase3, underTest);
  }

  @Test
  public void testFailuresAndResumes() throws URISyntaxException {

    URI uri = new URI("test://host/path");
    URI renamedTo = new URI("test://host/frog");
    SyncTask createDirectory = SyncTask.createDirectory(uri, syncMountId);
    SyncTask createFile = createFileSyncTask(uri, 1024, 1, 1);
    SyncTask deleteFile =
        SyncTask.deleteFile(uri, Collections.emptyList(), syncMountId);
    SyncTask deleteDirectory = SyncTask.deleteDirectory(uri, syncMountId);
    SyncTask renameFile = SyncTask.renameFile(
        uri, renamedTo, Collections.emptyList(), syncMountId);
    SyncTask renameDirectory =
        SyncTask.renameDirectory(uri, renamedTo, syncMountId);

    MetadataSyncTask expectedRenameFile = MetadataSyncTask.renameFile(
        (SyncTask.RenameFileSyncTask) renameFile
    );
    MetadataSyncTask expectedCreateDirectory =
        MetadataSyncTask.createDirectory(
            (SyncTask.CreateDirectorySyncTask) createDirectory
    );
    MetadataSyncTask expectedDeleteFile = MetadataSyncTask.deleteFile(
        (SyncTask.DeleteFileSyncTask) deleteFile
    );
    MetadataSyncTask expectedDeleteDirectory = MetadataSyncTask.deleteDirectory(
        (SyncTask.DeleteDirectorySyncTask) deleteDirectory
    );
    MetadataSyncTask expectedRenameDirectory = MetadataSyncTask.renameDirectory(
        (SyncTask.RenameDirectorySyncTask) renameDirectory
    );

    PhasedPlan phasedPlan = new PhasedPlan(
        Lists.newArrayList(renameFile),
        Lists.newArrayList(deleteDirectory, deleteFile),
        Lists.newArrayList(renameDirectory),
        Lists.newArrayList(createDirectory),
        Lists.newArrayList(createFile)
    );

    SyncMountSnapshotUpdateTrackerImpl underTest =
        new SyncMountSnapshotUpdateTrackerImpl(phasedPlan, dummyAliasMapWriter,
            configuration);
    assertThat(underTest.getCurrentPhase()).isEqualTo(NOT_STARTED);

    SchedulableSyncPhase renamesPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renamesPhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameFile))
    );

    markPhaseFailed(renamesPhase, underTest);

    SchedulableSyncPhase renamesRetryPhase1 =
        underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);
    assertThat(renamesRetryPhase1).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameFile))
    );

    markPhaseSuccessful(renamesRetryPhase1, underTest);

    SchedulableSyncPhase deletePhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(DELETES);
    assertThat(deletePhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedDeleteFile, expectedDeleteDirectory))
    );

    markPhaseSuccessful(deletePhase, underTest);

    SchedulableSyncPhase renameToFinalPhase =
        underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_FINAL);
    assertThat(renameToFinalPhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedRenameDirectory))
    );

    markPhaseSuccessful(renameToFinalPhase, underTest);

    SchedulableSyncPhase createDirPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_DIRS);
    assertThat(createDirPhase).isEqualTo(SchedulableSyncPhase.createMeta(
        Lists.newArrayList(expectedCreateDirectory))
    );

    markPhaseSuccessful(createDirPhase, underTest);

    SchedulableSyncPhase createFilePhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(INIT_PHASE);

    markPhaseFailed(createFilePhase, underTest);

    SchedulableSyncPhase createFileRetryPhase1 =
        underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(INIT_PHASE);

    markPhaseFailed(createFilePhase, underTest);

    SchedulableSyncPhase mpInitPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(INIT_PHASE);

    markPhaseSuccessful(mpInitPhase, underTest);

    SchedulableSyncPhase mpPutPartPhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(PUT_PHASE);

    markPhaseSuccessful(mpPutPartPhase, underTest);

    SchedulableSyncPhase mpCompletePhase = underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);
    assertThat(underTest.getMultipartPlanOpt().get().getMultipartPhase())
        .isEqualTo(COMPLETE_PHASE);

    markPhaseSuccessful(mpCompletePhase, underTest);

    SchedulableSyncPhase phase = underTest.getNextSchedulablePhase();
    assertThat(phase).isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(FINISHED);
    assertThat(underTest.isFinished()).isTrue();
  }

  @Test
  public void testMultipartCreateWithFailures() throws URISyntaxException {

    URI uri = new URI("test://host/path");
    long fileLength = 43L;
    SyncTask createFile = createFileSyncTask(uri, fileLength, 42L, 44L);

    PhasedPlan phasedPlan = new PhasedPlan(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Lists.newArrayList(createFile)
    );

    SyncMountSnapshotUpdateTrackerImpl underTest =
        new SyncMountSnapshotUpdateTrackerImpl(phasedPlan,
            dummyAliasMapWriter, configuration);
    assertThat(underTest.getCurrentPhase()).isEqualTo(NOT_STARTED);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(DELETES);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_FINAL);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_DIRS);

    SchedulableSyncPhase initMultipartPhase =
        underTest.getNextSchedulablePhase();
    assertThat(initMultipartPhase.getMetadataSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseFailed(initMultipartPhase, underTest);

    SchedulableSyncPhase initMultipartRetryPhase =
        underTest.getNextSchedulablePhase();
    assertThat(initMultipartRetryPhase.getMetadataSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(initMultipartRetryPhase, underTest);

    SchedulableSyncPhase putPartPhase = underTest.getNextSchedulablePhase();
    assertThat(putPartPhase.getBlockSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseFailed(putPartPhase, underTest);

    SchedulableSyncPhase putPartRetryPhase =
        underTest.getNextSchedulablePhase();
    assertThat(putPartRetryPhase.getBlockSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(putPartRetryPhase, underTest);

    SchedulableSyncPhase completeMultipartPhase =
        underTest.getNextSchedulablePhase();
    assertThat(completeMultipartPhase.getMetadataSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseFailed(completeMultipartPhase, underTest);

    SchedulableSyncPhase completeMultipartPhaseRetry =
        underTest.getNextSchedulablePhase();
    assertThat(completeMultipartPhaseRetry.getMetadataSyncTaskList())
        .hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(completeMultipartPhaseRetry, underTest);

    SchedulableSyncPhase phase = underTest.getNextSchedulablePhase();
    assertThat(phase).isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(FINISHED);
    assertThat(underTest.isFinished()).isTrue();

  }

  @Test
  public void testMultipartCreate() throws URISyntaxException {

    URI uri = new URI("test://host/path");
    long fileLength = 43L;
    SyncTask createFile = createFileSyncTask(uri, fileLength, 42L, 44L);

    PhasedPlan phasedPlan = new PhasedPlan(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Lists.newArrayList(createFile)
    );

    SyncMountSnapshotUpdateTrackerImpl underTest =
        new SyncMountSnapshotUpdateTrackerImpl(phasedPlan, dummyAliasMapWriter,
            configuration);
    assertThat(underTest.getCurrentPhase()).isEqualTo(NOT_STARTED);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(DELETES);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_FINAL);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_DIRS);

    SchedulableSyncPhase initMultipartPhase =
        underTest.getNextSchedulablePhase();
    assertThat(initMultipartPhase.getMetadataSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(initMultipartPhase, underTest);

    SchedulableSyncPhase putPartPhase = underTest.getNextSchedulablePhase();
    assertThat(putPartPhase.getBlockSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(putPartPhase, underTest);

    SchedulableSyncPhase completeMultipartPhase =
        underTest.getNextSchedulablePhase();
    assertThat(completeMultipartPhase.getMetadataSyncTaskList()).hasSize(1);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(completeMultipartPhase, underTest);

    SchedulableSyncPhase phase = underTest.getNextSchedulablePhase();

    assertThat(phase).isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(FINISHED);
    assertThat(underTest.isFinished()).isTrue();
  }

  @Test
  public void testMultipleMultipartCreates() throws URISyntaxException {

    URI uri1 = new URI("test://host/path1");
    URI uri2 = new URI("test://host/path2");
    URI uri3 = new URI("test://host/path3");
    URI uri4 = new URI("test://host/path4");
    URI uri5 = new URI("test://host/path5");

    SyncTask createFile1 = createFileSyncTask(uri1, 43L, 42L, 44L);
    SyncTask createFile2 = createFileSyncTask(uri2, 44L, 43L, 45L);
    SyncTask createFile3 = createFileSyncTask(uri3, 45L, 44L, 46L);
    SyncTask createFile4 = createFileSyncTask(uri4, 46L, 45L, 47L);
    SyncTask createFile5 = createFileSyncTask(uri5, 47L, 46L, 48L);

    PhasedPlan phasedPlan = new PhasedPlan(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Lists.newArrayList(createFile1, createFile2, createFile3,
            createFile4, createFile5)
    );

    SyncMountSnapshotUpdateTrackerImpl underTest =
        new SyncMountSnapshotUpdateTrackerImpl(phasedPlan, dummyAliasMapWriter,
            configuration);
    assertThat(underTest.getCurrentPhase()).isEqualTo(NOT_STARTED);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_TEMP);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(DELETES);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(RENAMES_TO_FINAL);

    underTest.getNextSchedulablePhase();
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_DIRS);

    SchedulableSyncPhase initMultipartPhase =
        underTest.getNextSchedulablePhase();
    assertThat(initMultipartPhase.getMetadataSyncTaskList()).hasSize(5);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(initMultipartPhase, underTest);

    SchedulableSyncPhase putPartPhase = underTest.getNextSchedulablePhase();
    assertThat(putPartPhase.getBlockSyncTaskList()).hasSize(5);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(putPartPhase, underTest);

    SchedulableSyncPhase completeMultipartPhase =
        underTest.getNextSchedulablePhase();
    assertThat(completeMultipartPhase.getMetadataSyncTaskList()).hasSize(5);
    assertThat(underTest.getCurrentPhase()).isEqualTo(CREATE_FILES);

    markPhaseSuccessful(completeMultipartPhase, underTest);

    SchedulableSyncPhase phase = underTest.getNextSchedulablePhase();

    assertThat(phase).isEqualTo(SchedulableSyncPhase.empty());
    assertThat(underTest.getCurrentPhase()).isEqualTo(FINISHED);
    assertThat(underTest.isFinished()).isTrue();
  }

  private SyncTask createFileSyncTask(URI uri, long fileLength, long blkid,
      long generationStamp) {
    long blockCollectionId = 42L;
    Block block1 = new Block(blkid, fileLength, generationStamp);
    LocatedBlocks locatedBlocks1 = createLocatedBlocks(fileLength, block1);
    return SyncTask.createFile(uri, syncMountId,
        locatedBlocks1.getLocatedBlocks(), blockCollectionId);
  }

  private void markPhaseSuccessful(SchedulableSyncPhase phase,
      SyncMountSnapshotUpdateTrackerImpl underTest) {
    phase.getMetadataSyncTaskList().forEach(
        metadataSyncTask -> underTest.markFinished(
            metadataSyncTask.getSyncTaskId(),
            SyncTaskExecutionResult.emptyResult()));

    phase.getBlockSyncTaskList().forEach(
        blockSyncTask -> underTest.markFinished(blockSyncTask.getSyncTaskId(),
            SyncTaskExecutionResult.emptyResult()));
  }

  private void markPhaseFailed(SchedulableSyncPhase phase,
      SyncMountSnapshotUpdateTrackerImpl underTest) {
    phase.getMetadataSyncTaskList().forEach(
        metadataSyncTask -> underTest.markFailed(
            metadataSyncTask.getSyncTaskId(),
            SyncTaskExecutionResult.emptyResult()));

    phase.getBlockSyncTaskList().forEach(
        blockSyncTask -> underTest.markFailed(blockSyncTask.getSyncTaskId(),
            SyncTaskExecutionResult.emptyResult()));
  }

  private LocatedBlocks createLocatedBlocks(long fileLength, Block block) {
    ExtendedBlock extendedBlock = new ExtendedBlock("poolId", block);
    LocatedBlock locatedBlock = new LocatedBlock(extendedBlock, null);
    List<LocatedBlock> blocks = Lists.newArrayList(locatedBlock);
    return new LocatedBlocks(fileLength, false,
        blocks, null, true, null, null);
  }
}
