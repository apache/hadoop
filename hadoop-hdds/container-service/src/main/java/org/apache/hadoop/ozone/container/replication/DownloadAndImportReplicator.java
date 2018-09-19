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
package org.apache.hadoop.ozone.container.replication;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default replication implementation.
 * <p>
 * This class does the real job. Executes the download and import the container
 * to the container set.
 */
public class DownloadAndImportReplicator implements ContainerReplicator {

  private static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndImportReplicator.class);

  private final ContainerSet containerSet;

  private final ContainerDispatcher containerDispatcher;

  private final ContainerDownloader downloader;

  private final TarContainerPacker packer;

  public DownloadAndImportReplicator(
      ContainerSet containerSet,
      ContainerDispatcher containerDispatcher,
      ContainerDownloader downloader,
      TarContainerPacker packer) {
    this.containerSet = containerSet;
    this.containerDispatcher = containerDispatcher;
    this.downloader = downloader;
    this.packer = packer;
  }

  public void importContainer(long containerID, Path tarFilePath) {
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
    }
  }

  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();

    List<DatanodeDetails> sourceDatanodes = task.getSources();

    LOG.info("Starting replication of container {} from {}", containerID,
        sourceDatanodes);

    CompletableFuture<Path> tempTarFile = downloader
        .getContainerDataFromReplicas(containerID,
            sourceDatanodes);

    try {
      //wait for the download. This thread pool is limiting the paralell
      //downloads, so it's ok to block here and wait for the full download.
      Path path = tempTarFile.get();
      LOG.info("Container {} is downloaded, starting to import.",
          containerID);
      importContainer(containerID, path);
      LOG.info("Container {} is replicated successfully", containerID);
      task.setStatus(Status.DONE);
    } catch (Exception e) {
      LOG.error("Container replication was unsuccessful .", e);
      task.setStatus(Status.FAILED);
    }
  }
}
