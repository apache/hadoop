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

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ContainerDownloader;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command handler to copy containers from sources.
 */
public class ReplicateContainerCommandHandler implements CommandHandler {

  static final Logger LOG =
      LoggerFactory.getLogger(ReplicateContainerCommandHandler.class);

  private ContainerDispatcher containerDispatcher;

  private int invocationCount;

  private long totalTime;

  private ContainerDownloader downloader;

  private Configuration conf;

  private TarContainerPacker packer = new TarContainerPacker();

  private ContainerSet containerSet;

  private Lock lock = new ReentrantLock();

  public ReplicateContainerCommandHandler(
      Configuration conf,
      ContainerSet containerSet,
      ContainerDispatcher containerDispatcher,
      ContainerDownloader downloader) {
    this.conf = conf;
    this.containerSet = containerSet;
    this.downloader = downloader;
    this.containerDispatcher = containerDispatcher;
  }

  public ReplicateContainerCommandHandler(
      Configuration conf,
      ContainerSet containerSet,
      ContainerDispatcher containerDispatcher) {
    this(conf, containerSet, containerDispatcher,
        new SimpleContainerDownloader(conf));
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {

    ReplicateContainerCommand replicateCommand =
        (ReplicateContainerCommand) command;
    try {

      long containerID = replicateCommand.getContainerID();
      LOG.info("Starting replication of container {} from {}", containerID,
          replicateCommand.getSourceDatanodes());
      CompletableFuture<Path> tempTarFile = downloader
          .getContainerDataFromReplicas(containerID,
              replicateCommand.getSourceDatanodes());

      CompletableFuture<Void> result =
          tempTarFile.thenAccept(path -> {
            LOG.info("Container {} is downloaded, starting to import.",
                containerID);
            importContainer(containerID, path);
          });

      result.whenComplete((aVoid, throwable) -> {
        if (throwable != null) {
          LOG.error("Container replication was unsuccessful .", throwable);
        } else {
          LOG.info("Container {} is replicated successfully", containerID);
        }
      });
    } finally {
      updateCommandStatus(context, command, true, LOG);

    }
  }

  protected void importContainer(long containerID, Path tarFilePath) {
    lock.lock();
    try {
      ContainerData originalContainerData;
      try (FileInputStream tempContainerTarStream = new FileInputStream(
          tarFilePath.toFile())) {
        byte[] containerDescriptorYaml =
            packer.unpackContainerDescriptor(tempContainerTarStream);
        originalContainerData = ContainerDataYaml.readContainer(
            containerDescriptorYaml);
      }

      try (FileInputStream tempContainerTarStream = new FileInputStream(
          tarFilePath.toFile())) {

        Handler handler = containerDispatcher.getHandler(
            originalContainerData.getContainerType());

        Container container = handler.importContainer(containerID,
            originalContainerData.getMaxSize(),
            tempContainerTarStream,
            packer);

        containerSet.addContainer(container);
      }

    } catch (Exception e) {
      LOG.error(
          "Can't import the downloaded container data id=" + containerID,
          e);
      try {
        Files.delete(tarFilePath);
      } catch (Exception ex) {
        LOG.error(
            "Container import is failed and the downloaded file can't be "
                + "deleted: "
                + tarFilePath.toAbsolutePath().toString());
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return Type.replicateContainerCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount;
  }

  @Override
  public long getAverageRunTime() {
    if (invocationCount > 0) {
      return totalTime / invocationCount;
    }
    return 0;
  }
}
