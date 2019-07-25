/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test methods of ContainerStateMachine.
 */
public class TestContainerStateMachine {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDeleteOldSnapshot() throws Exception {

    File firstSnapshotFile = temporaryFolder.newFile();
    File secondSnapshotFile = temporaryFolder.newFile();
    File thirdSnapshotFile = temporaryFolder.newFile();

    FileInfo fileInfoMock = mock(FileInfo.class);
    when(fileInfoMock.getPath()).thenReturn(firstSnapshotFile.toPath())
        .thenReturn(secondSnapshotFile.toPath());

    SingleFileSnapshotInfo singleFileSnapshotInfoMock = mock(
        SingleFileSnapshotInfo.class);
    when(singleFileSnapshotInfoMock.getFile()).thenReturn(fileInfoMock);

    TermIndex termIndexMock = mock(TermIndex.class);
    when(termIndexMock.getIndex()).thenReturn(1L);
    when(termIndexMock.getTerm()).thenReturn(1L);

    SimpleStateMachineStorage simpleStateMachineStorageMock =
        mock(SimpleStateMachineStorage.class);
    when(simpleStateMachineStorageMock.findLatestSnapshot())
        .thenReturn(singleFileSnapshotInfoMock);
    when(simpleStateMachineStorageMock.getSnapshotFile(1L, 1L))
        .thenReturn(secondSnapshotFile)
        .thenReturn(thirdSnapshotFile)
        //Return non-existent file while taking 3rd snapshot.
        .thenReturn(Paths.get("NonExistentDir", "NonExistentFile")
            .toFile());

    ContainerStateMachine containerStateMachine =
        mock(ContainerStateMachine.class);
    when(containerStateMachine.getLastAppliedTermIndex()).thenReturn(
        termIndexMock);
    when(containerStateMachine.takeSnapshot()).thenCallRealMethod();

    // Have to use reflections here since storage is baked into
    // ContainerStateMachine class.
    Field f1 = containerStateMachine.getClass().getSuperclass()
        .getDeclaredField("storage");
    f1.setAccessible(true);
    f1.set(containerStateMachine, simpleStateMachineStorageMock);

    // Verify last snapshot deletion while calling takeSnapshot() API.
    assertTrue(firstSnapshotFile.exists());
    containerStateMachine.takeSnapshot();
    assertFalse(firstSnapshotFile.exists());

    // Verify current snapshot deletion while calling takeSnapshot() API once
    // more.
    assertTrue(secondSnapshotFile.exists());
    containerStateMachine.takeSnapshot();
    assertFalse(secondSnapshotFile.exists());

    // Now, takeSnapshot throws IOException.
    try {
      containerStateMachine.takeSnapshot();
      Assert.fail();
    } catch (IOException ioEx) {
      //Verify the old snapshot file still exists.
      assertTrue(thirdSnapshotFile.exists());
    }

  }
}