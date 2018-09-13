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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.replication.ContainerDownloader;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.TestGenericTestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test replication command handler.
 */
public class TestReplicateContainerCommandHandler {

  private static final String EXCEPTION_MESSAGE = "Oh my god";
  private ReplicateContainerCommandHandler handler;
  private StubDownloader downloader;
  private ReplicateContainerCommand command;
  private List<Long> importedContainerIds;

  @Before
  public void init() {
    importedContainerIds = new ArrayList<>();

    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerSet containerSet = Mockito.mock(ContainerSet.class);
    ContainerDispatcher containerDispatcher =
        Mockito.mock(ContainerDispatcher.class);

    downloader = new StubDownloader();

    handler = new ReplicateContainerCommandHandler(conf, containerSet,
        containerDispatcher, downloader) {
      @Override
      protected void importContainer(long containerID, Path tarFilePath) {
        importedContainerIds.add(containerID);
      }
    };

    //the command
    ArrayList<DatanodeDetails> datanodeDetails = new ArrayList<>();
    datanodeDetails.add(Mockito.mock(DatanodeDetails.class));
    datanodeDetails.add(Mockito.mock(DatanodeDetails.class));

    command = new ReplicateContainerCommand(1L, datanodeDetails);
  }

  @Test
  public void handle() throws TimeoutException, InterruptedException {
    //GIVEN

    //WHEN
    handler.handle(command, null, Mockito.mock(StateContext.class), null);

    TestGenericTestUtils
        .waitFor(() -> downloader.futureByContainers.size() == 1, 100, 2000);

    Assert.assertNotNull(downloader.futureByContainers.get(1L));
    downloader.futureByContainers.get(1L).complete(Paths.get("/tmp/test"));

    TestGenericTestUtils
        .waitFor(() -> importedContainerIds.size() == 1, 100, 2000);
  }

  @Test
  public void handleWithErrors() throws TimeoutException, InterruptedException {
    //GIVEN
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(ReplicateContainerCommandHandler.LOG);

    //WHEN
    handler.handle(command, null, Mockito.mock(StateContext.class), null);

    //THEN
    TestGenericTestUtils
        .waitFor(() -> downloader.futureByContainers.size() == 1, 100, 2000);

    Assert.assertNotNull(downloader.futureByContainers.get(1L));
    downloader.futureByContainers.get(1L)
        .completeExceptionally(new IllegalArgumentException(
            EXCEPTION_MESSAGE));

    TestGenericTestUtils
        .waitFor(() -> {
          String output = logCapturer.getOutput();
          return output.contains("unsuccessful") && output
            .contains(EXCEPTION_MESSAGE); },
            100,
            2000);
  }

  /**
   * Can't handle a command if there are no source replicas.
   */
  @Test(expected = IllegalArgumentException.class)
  public void handleWithoutReplicas()
      throws TimeoutException, InterruptedException {
    //GIVEN
    ReplicateContainerCommand commandWithoutReplicas =
        new ReplicateContainerCommand(1L, new ArrayList<>());

    //WHEN
    handler
        .handle(commandWithoutReplicas,
            null,
            Mockito.mock(StateContext.class),
            null);

  }
  private static class StubDownloader implements ContainerDownloader {

    private Map<Long, CompletableFuture<Path>> futureByContainers =
        new HashMap<>();

    @Override
    public void close() {

    }

    @Override
    public CompletableFuture<Path> getContainerDataFromReplicas(
        long containerId, List<DatanodeDetails> sources) {
      CompletableFuture<Path> future = new CompletableFuture<>();
      futureByContainers.put(containerId, future);
      return future;
    }
  }

}