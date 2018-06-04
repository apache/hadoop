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
package org.apache.hadoop.ozone.container.common.states.endpoint;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.DataNodeLayoutVersion;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Task that returns version.
 */
public class VersionEndpointTask implements
    Callable<EndpointStateMachine.EndPointStates> {
  private final EndpointStateMachine rpcEndPoint;
  private final Configuration configuration;
  private final OzoneContainer datanodeContainerManager;
  static final Logger LOG =
      LoggerFactory.getLogger(VersionEndpointTask.class);

  public VersionEndpointTask(EndpointStateMachine rpcEndPoint,
                             Configuration conf, OzoneContainer container) {
    this.rpcEndPoint = rpcEndPoint;
    this.configuration = conf;
    this.datanodeContainerManager = container;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStateMachine.EndPointStates call() throws Exception {
    rpcEndPoint.lock();
    try {
      SCMVersionResponseProto versionResponse =
          rpcEndPoint.getEndPoint().getVersion(null);
      VersionResponse response = VersionResponse.getFromProtobuf(
          versionResponse);
      String scmUuid = response.getValue(OzoneConsts.SCM_ID);
      Preconditions.checkState(!StringUtils.isBlank(scmUuid),
          "Invalid SCM UuiD in the response.");

      rpcEndPoint.setVersion(response);
      LOG.debug("scmUuid is {}", scmUuid);

      List<StorageLocation> locations = datanodeContainerManager.getLocations();

      for (StorageLocation location : locations) {
        String path = location.getUri().getPath();
        File parentPath = new File(path + File.separator + Storage
            .STORAGE_DIR_HDDS + File.separator + scmUuid + File.separator +
            Storage.STORAGE_DIR_CURRENT);
        File versionFile = DatanodeVersionFile.getVersionFile(location,
            scmUuid);
        if (!parentPath.exists() && !parentPath.mkdirs()) {
          LOG.error("Directory doesn't exist and cannot be created. Path: {}",
              parentPath.toString());
          rpcEndPoint.setState(EndpointStateMachine.EndPointStates.SHUTDOWN);
          throw new IllegalArgumentException("Directory doesn't exist and " +
              "cannot be created. " + parentPath.toString());
        } else {
          if (versionFile.exists()) {
            Properties properties = DatanodeVersionFile.readFrom(versionFile);
            DatanodeVersionFile.verifyScmUuid(properties.getProperty(
                OzoneConsts.SCM_ID), scmUuid);
            DatanodeVersionFile.verifyCreationTime(properties.getProperty(
                OzoneConsts.CTIME));
            DatanodeVersionFile.verifyLayOutVersion(properties.getProperty(
                OzoneConsts.LAYOUTVERSION));
          } else {
            DatanodeVersionFile dnVersionFile = new DatanodeVersionFile(scmUuid,
                Time.now(), DataNodeLayoutVersion.getLatestVersion()
                .getVersion());
            dnVersionFile.createVersionFile(versionFile);
          }
        }
      }
      EndpointStateMachine.EndPointStates nextState = rpcEndPoint.getState().
          getNextState();
      rpcEndPoint.setState(nextState);
      rpcEndPoint.zeroMissedCount();
    } catch (InconsistentStorageStateException ex) {
      throw ex;
    } catch (IOException ex) {
      rpcEndPoint.logIfNeeded(ex);
    } finally {
      rpcEndPoint.unlock();
    }
    return rpcEndPoint.getState();
  }
}
