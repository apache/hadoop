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
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_SIZE;
import org.junit.Test;

/**
 * Tests the behavior of the datanode, when replicate container command is
 * received.
 */
public class TestReplicateContainerHandler {

  @Test
  public void test() throws IOException, TimeoutException, InterruptedException,
      OzoneException {

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(ReplicateContainerCommandHandler.LOG);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    MiniOzoneCluster cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();

    DatanodeDetails datanodeDetails =
        cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    //send the order to replicate the container
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getUuid(),
            new ReplicateContainerCommand(1L,
                new ArrayList<>()));

    //TODO: here we test only the serialization/unserialization as
    // the implementation is not yet done
    GenericTestUtils
        .waitFor(() -> logCapturer.getOutput().contains("not yet handled"), 500,
            5 * 1000);

  }

}