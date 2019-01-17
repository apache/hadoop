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

package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple ContainerDownloaderImplementation to download the missing container
 * from the first available datanode.
 * <p>
 * This is not the most effective implementation as it uses only one source
 * for he container download.
 */
public class SimpleContainerDownloader implements ContainerDownloader {

  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleContainerDownloader.class);

  private final Path workingDirectory;

  private ExecutorService executor;

  public SimpleContainerDownloader(Configuration conf) {

    String workDirString =
        conf.get(OzoneConfigKeys.OZONE_CONTAINER_COPY_WORKDIR);

    if (workDirString == null) {
      workingDirectory = Paths.get(System.getProperty("java.io.tmpdir"))
          .resolve("container-copy");
    } else {
      workingDirectory = Paths.get(workDirString);
    }

    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Container downloader thread - %d").build();
    executor = Executors.newSingleThreadExecutor(build);
    LOG.info("Starting container downloader service to copy "
        + "containers to replicate.");
  }

  @Override
  public CompletableFuture<Path> getContainerDataFromReplicas(long containerId,
      List<DatanodeDetails> sourceDatanodes) {

    CompletableFuture<Path> result = null;
    for (DatanodeDetails datanode : sourceDatanodes) {
      try {

        if (result == null) {
          GrpcReplicationClient grpcReplicationClient =
              new GrpcReplicationClient(datanode.getIpAddress(),
                  datanode.getPort(Name.STANDALONE).getValue(),
                  workingDirectory);
          result = grpcReplicationClient.download(containerId);
        } else {
          result = result.thenApply(CompletableFuture::completedFuture)
              .exceptionally(t -> {
                LOG.error("Error on replicating container: " + containerId, t);
                GrpcReplicationClient grpcReplicationClient =
                    new GrpcReplicationClient(datanode.getIpAddress(),
                        datanode.getPort(Name.STANDALONE).getValue(),
                        workingDirectory);
                return grpcReplicationClient.download(containerId);
              }).thenCompose(Function.identity());
        }
      } catch (Exception ex) {
        LOG.error(String.format(
            "Container %s download from datanode %s was unsuccessful. "
                + "Trying the next datanode", containerId, datanode), ex);
      }

    }
    return result;

  }

  @Override
  public void close() throws IOException {
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Can't stop container downloader gracefully", e);
    }
  }
}
