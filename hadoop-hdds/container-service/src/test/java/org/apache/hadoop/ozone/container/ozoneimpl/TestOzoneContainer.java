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


import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.Random;
import java.util.UUID;


import static org.junit.Assert.assertEquals;

/**
 * This class is used to test OzoneContainer.
 */
public class TestOzoneContainer {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  private OzoneConfiguration conf;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private final DatanodeDetails datanodeDetails = createDatanodeDetails();

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.getRoot()
        .getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.newFolder().getAbsolutePath());
  }

  @Test
  public void testBuildContainerMap() throws Exception {
    volumeSet = new VolumeSet(datanodeDetails.getUuidString(), conf);
    volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();

    // Format the volumes
    for (HddsVolume volume : volumeSet.getVolumesList()) {
      volume.format(UUID.randomUUID().toString());
    }

    // Add containers to disk
    for (int i=0; i<10; i++) {
      keyValueContainerData = new KeyValueContainerData(i,
          (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
          datanodeDetails.getUuidString());
      keyValueContainer = new KeyValueContainer(
          keyValueContainerData, conf);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    }

    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    // When OzoneContainer is started, the containers from disk should be
    // loaded into the containerSet.
    OzoneContainer ozoneContainer = new
        OzoneContainer(datanodeDetails, conf, context, null);
    ContainerSet containerset = ozoneContainer.getContainerSet();
    assertEquals(10, containerset.containerCount());
  }


  private DatanodeDetails createDatanodeDetails() {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    String uuid = UUID.randomUUID().toString();
    String hostName = uuid;
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }
}
