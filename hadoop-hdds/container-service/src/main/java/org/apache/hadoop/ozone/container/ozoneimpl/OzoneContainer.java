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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;

import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

import static org.apache.hadoop.ozone.OzoneConsts.INVALID_PORT;

/**
 * Ozone main class sets up the network server and initializes the container
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
  private final XceiverServerSpi[] server;

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
    boolean useGrpc = this.config.getBoolean(
        ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_DEFAULT);
    buildContainerSet();
    hddsDispatcher = new HddsDispatcher(config, containerSet, volumeSet,
        context);
    server = new XceiverServerSpi[]{
        useGrpc ? new XceiverServerGrpc(datanodeDetails, this.config, this
            .hddsDispatcher) :
            new XceiverServer(datanodeDetails,
                this.config, this.hddsDispatcher),
        XceiverServerRatis.newXceiverServerRatis(datanodeDetails, this
            .config, hddsDispatcher)
    };


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
    for (XceiverServerSpi serverinstance : server) {
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
    for(XceiverServerSpi serverinstance: server) {
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

  /**
   * Submit ContainerRequest.
   * @param request
   * @param replicationType
   * @param pipelineID
   * @throws IOException
   */
  public void submitContainerRequest(
      ContainerProtos.ContainerCommandRequestProto request,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.PipelineID pipelineID) throws IOException {
    XceiverServerSpi serverInstance;
    long containerId = getContainerIdForCmd(request);
    if (replicationType == HddsProtos.ReplicationType.RATIS) {
      serverInstance = getRatisSerer();
      Preconditions.checkNotNull(serverInstance);
      serverInstance.submitRequest(request, pipelineID);
      LOG.info("submitting {} request over RATIS server for container {}",
          request.getCmdType(), containerId);
    } else {
      serverInstance = getStandaAloneSerer();
      Preconditions.checkNotNull(serverInstance);
      getStandaAloneSerer().submitRequest(request, pipelineID);
      LOG.info(
          "submitting {} request over STAND_ALONE server for container {}",
          request.getCmdType(), containerId);
    }

  }

  private long getContainerIdForCmd(
      ContainerProtos.ContainerCommandRequestProto request)
      throws IllegalArgumentException {
    ContainerProtos.Type type = request.getCmdType();
    switch (type) {
    case CloseContainer:
      return request.getContainerID();
      // Right now, we handle only closeContainer via queuing it over the
      // over the XceiVerServer. For all other commands we throw Illegal
      // argument exception here. Will need to extend the switch cases
      // in case we want add another commands here.
    default:
      throw new IllegalArgumentException("Cmd " + request.getCmdType()
          + " not supported over HearBeat Response");
    }
  }

  private XceiverServerSpi getRatisSerer() {
    for (XceiverServerSpi serverInstance : server) {
      if (serverInstance instanceof XceiverServerRatis) {
        return serverInstance;
      }
    }
    return null;
  }

  private XceiverServerSpi getStandaAloneSerer() {
    for (XceiverServerSpi serverInstance : server) {
      if (!(serverInstance instanceof XceiverServerRatis)) {
        return serverInstance;
      }
    }
    return null;
  }

  private int getPortbyType(HddsProtos.ReplicationType replicationType) {
    for (XceiverServerSpi serverinstance : server) {
      if (serverinstance.getServerType() == replicationType) {
        return serverinstance.getIPCPort();
      }
    }
    return INVALID_PORT;
  }

  /**
   * Returns the container server IPC port.
   *
   * @return Container server IPC port.
   */
  public int getContainerServerPort() {
    return getPortbyType(HddsProtos.ReplicationType.STAND_ALONE);
  }

  /**
   * Returns the Ratis container Server IPC port.
   *
   * @return Ratis port.
   */
  public int getRatisContainerServerPort() {
    return getPortbyType(HddsProtos.ReplicationType.RATIS);
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
