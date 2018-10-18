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

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;

import org.apache.hadoop.ozone.container.replication.GrpcReplicationService;
import org.apache.hadoop.ozone.container.replication
    .OnDemandContainerReplicationSource;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.INVALID_PORT;

/**
 * Ozone main class sets up the network servers and initializes the container
 * layer.
 */
public class OzoneContainer {

  public static final Logger LOG = LoggerFactory.getLogger(
      OzoneContainer.class);

  private final HddsDispatcher hddsDispatcher;
  private final DatanodeDetails dnDetails;
  private final OzoneConfiguration config;
  private final VolumeSet volumeSet;
  private final ContainerSet containerSet;
  private final Map<ReplicationType, XceiverServerSpi> servers;

  /**
   * Construct OzoneContainer object.
   * @param datanodeDetails
   * @param conf
   * @throws DiskOutOfSpaceException
   * @throws IOException
   */
  public OzoneContainer(DatanodeDetails datanodeDetails, OzoneConfiguration
      conf, StateContext context) throws IOException {
    this.dnDetails = datanodeDetails;
    this.config = conf;
    this.volumeSet = new VolumeSet(datanodeDetails.getUuidString(), conf);
    this.containerSet = new ContainerSet();
    buildContainerSet();
    hddsDispatcher = new HddsDispatcher(config, containerSet, volumeSet,
        context);
    servers = new HashMap<>();
    servers.put(ReplicationType.STAND_ALONE,
        new XceiverServerGrpc(datanodeDetails, config, hddsDispatcher,
            createReplicationService()));
    servers.put(ReplicationType.RATIS, XceiverServerRatis
        .newXceiverServerRatis(datanodeDetails, config, hddsDispatcher,
            context));
  }

  private GrpcReplicationService createReplicationService() {
    return new GrpcReplicationService(
        new OnDemandContainerReplicationSource(containerSet));
  }

  /**
   * Build's container map.
   */
  public void buildContainerSet() {
    Iterator<HddsVolume> volumeSetIterator = volumeSet.getVolumesList()
        .iterator();
    ArrayList<Thread> volumeThreads = new ArrayList<Thread>();

    //TODO: diskchecker should be run before this, to see how disks are.
    // And also handle disk failure tolerance need to be added
    while (volumeSetIterator.hasNext()) {
      HddsVolume volume = volumeSetIterator.next();
      File hddsVolumeRootDir = volume.getHddsRootDir();
      Thread thread = new Thread(new ContainerReader(volumeSet, volume,
          containerSet, config));
      thread.start();
      volumeThreads.add(thread);
    }

    try {
      for (int i = 0; i < volumeThreads.size(); i++) {
        volumeThreads.get(i).join();
      }
    } catch (InterruptedException ex) {
      LOG.info("Volume Threads Interrupted exception", ex);
    }

  }

  /**
   * Starts serving requests to ozone container.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    LOG.info("Attempting to start container services.");
    for (XceiverServerSpi serverinstance : servers.values()) {
      serverinstance.start();
    }
    hddsDispatcher.init();
  }

  /**
   * Stop Container Service on the datanode.
   */
  public void stop() {
    //TODO: at end of container IO integration work.
    LOG.info("Attempting to stop container services.");
    for(XceiverServerSpi serverinstance: servers.values()) {
      serverinstance.stop();
    }
    hddsDispatcher.shutdown();
  }


  @VisibleForTesting
  public ContainerSet getContainerSet() {
    return containerSet;
  }
  /**
   * Returns container report.
   * @return - container report.
   * @throws IOException
   */
  public StorageContainerDatanodeProtocolProtos.ContainerReportsProto
      getContainerReport() throws IOException {
    return this.containerSet.getContainerReport();
  }

  public PipelineReportsProto getPipelineReport() {
    PipelineReportsProto.Builder pipelineReportsProto =
            PipelineReportsProto.newBuilder();
    for (XceiverServerSpi serverInstance : servers.values()) {
      pipelineReportsProto
              .addAllPipelineReport(serverInstance.getPipelineReport());
    }
    return pipelineReportsProto.build();
  }

  /**
   * Submit ContainerRequest.
   * @param request
   * @param replicationType
   * @param pipelineID
   */
  public void submitContainerRequest(
      ContainerProtos.ContainerCommandRequestProto request,
      ReplicationType replicationType,
      PipelineID pipelineID) throws IOException {
    LOG.info("submitting {} request over {} server for container {}",
        request.getCmdType(), replicationType, request.getContainerID());
    Preconditions.checkState(servers.containsKey(replicationType));
    servers.get(replicationType).submitRequest(request, pipelineID);
  }

  private int getPortByType(ReplicationType replicationType) {
    return servers.containsKey(replicationType) ?
        servers.get(replicationType).getIPCPort() : INVALID_PORT;
  }

  /**
   * Returns the container servers IPC port.
   *
   * @return Container servers IPC port.
   */
  public int getContainerServerPort() {
    return getPortByType(ReplicationType.STAND_ALONE);
  }

  /**
   * Returns the Ratis container Server IPC port.
   *
   * @return Ratis port.
   */
  public int getRatisContainerServerPort() {
    return getPortByType(ReplicationType.RATIS);
  }

  /**
   * Returns node report of container storage usage.
   */
  public StorageContainerDatanodeProtocolProtos.NodeReportProto getNodeReport()
      throws IOException {
    return volumeSet.getNodeReport();
  }

  @VisibleForTesting
  public ContainerDispatcher getDispatcher() {
    return this.hddsDispatcher;
  }

  public VolumeSet getVolumeSet() {
    return volumeSet;
  }
}
