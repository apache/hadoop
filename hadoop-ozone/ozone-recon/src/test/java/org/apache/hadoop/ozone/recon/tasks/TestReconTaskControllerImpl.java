/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractSqlDatabaseTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOmMetadataManagerImpl;
import org.hadoop.ozone.recon.schema.ReconInternalSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.jooq.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Class used to test ReconTaskControllerImpl.
 */
public class TestReconTaskControllerImpl extends AbstractSqlDatabaseTest {

  private ReconTaskController reconTaskController;
  private Configuration sqlConfiguration;
  private ReconOMMetadataManager omMetadataManager;

  @Before
  public void setUp() throws Exception {

    File omDbDir = temporaryFolder.newFolder();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_DB_DIRS, omDbDir.getAbsolutePath());
    omMetadataManager = new ReconOmMetadataManagerImpl(ozoneConfiguration);

    sqlConfiguration = getInjector()
        .getInstance(Configuration.class);

    ReconInternalSchemaDefinition schemaDefinition = getInjector().
        getInstance(ReconInternalSchemaDefinition.class);
    schemaDefinition.initializeSchema();

    reconTaskController = new ReconTaskControllerImpl(ozoneConfiguration,
        omMetadataManager, sqlConfiguration, new HashSet<>());
  }

  @Test
  public void testRegisterTask() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconDBTask =
        new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.ALWAYS_PASS);
    reconTaskController.registerTask(dummyReconDBTask);
    assertTrue(reconTaskController.getRegisteredTasks().size() == 1);
    assertTrue(reconTaskController.getRegisteredTasks()
        .get(dummyReconDBTask.getTaskName()) == dummyReconDBTask);
  }

  @Test
  public void testConsumeOMEvents() throws Exception {

    ReconDBUpdateTask reconDBUpdateTaskMock = getMockTask("MockTask");
    when(reconDBUpdateTaskMock.process(any(OMUpdateEventBatch.class)))
        .thenReturn(new ImmutablePair<>("MockTask", true));
    reconTaskController.registerTask(reconDBUpdateTaskMock);
    reconTaskController.consumeOMEvents(
        new OMUpdateEventBatch(Collections.emptyList()));

    verify(reconDBUpdateTaskMock, times(1))
        .process(any());
  }

  @Test
  public void testFailedTaskRetryLogic() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconDBTask =
        new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.FAIL_ONCE);
    reconTaskController.registerTask(dummyReconDBTask);

    long currentTime = System.nanoTime();
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);

    reconTaskController.consumeOMEvents(omUpdateEventBatchMock);
    assertFalse(reconTaskController.getRegisteredTasks().isEmpty());
    assertEquals(dummyReconDBTask, reconTaskController.getRegisteredTasks()
        .get(dummyReconDBTask.getTaskName()));

    ReconTaskStatusDao dao = new ReconTaskStatusDao(sqlConfiguration);
    ReconTaskStatus dbRecord = dao.findById(taskName);

    Assert.assertEquals(taskName, dbRecord.getTaskName());
    Assert.assertTrue(
        dbRecord.getLastUpdatedTimestamp() > currentTime);
    Assert.assertEquals(Long.valueOf(100L), dbRecord.getLastUpdatedSeqNumber());
  }

  @Test
  public void testBadBehavedTaskBlacklisting() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconDBTask =
        new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.ALWAYS_FAIL);
    reconTaskController.registerTask(dummyReconDBTask);


    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);

    for (int i = 0; i < 2; i++) {
      reconTaskController.consumeOMEvents(omUpdateEventBatchMock);

      assertFalse(reconTaskController.getRegisteredTasks().isEmpty());
      assertEquals(dummyReconDBTask, reconTaskController.getRegisteredTasks()
          .get(dummyReconDBTask.getTaskName()));
    }

    //Should be blacklisted now.
    reconTaskController.consumeOMEvents(
        new OMUpdateEventBatch(Collections.emptyList()));
    assertTrue(reconTaskController.getRegisteredTasks().isEmpty());

    ReconTaskStatusDao dao = new ReconTaskStatusDao(sqlConfiguration);
    ReconTaskStatus dbRecord = dao.findById(taskName);

    Assert.assertEquals(taskName, dbRecord.getTaskName());
    Assert.assertEquals(Long.valueOf(0L), dbRecord.getLastUpdatedTimestamp());
    Assert.assertEquals(Long.valueOf(0L), dbRecord.getLastUpdatedSeqNumber());
  }


  @Test
  public void testReInitializeTasks() throws Exception {

    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    ReconDBUpdateTask reconDBUpdateTaskMock =
        getMockTask("MockTask2");
    when(reconDBUpdateTaskMock.reprocess(omMetadataManagerMock))
        .thenReturn(new ImmutablePair<>("MockTask2", true));

    reconTaskController.registerTask(reconDBUpdateTaskMock);
    reconTaskController.reInitializeTasks(omMetadataManagerMock);

    verify(reconDBUpdateTaskMock, times(1))
        .reprocess(omMetadataManagerMock);
  }

  /**
   * Helper method for getting a mocked Task.
   * @param taskName name of the task.
   * @return instance of ReconDBUpdateTask.
   */
  private ReconDBUpdateTask getMockTask(String taskName) {
    ReconDBUpdateTask reconDBUpdateTaskMock = mock(ReconDBUpdateTask.class);
    when(reconDBUpdateTaskMock.getTaskTables()).thenReturn(Collections
        .EMPTY_LIST);
    when(reconDBUpdateTaskMock.getTaskName()).thenReturn(taskName);
    return reconDBUpdateTaskMock;
  }
}