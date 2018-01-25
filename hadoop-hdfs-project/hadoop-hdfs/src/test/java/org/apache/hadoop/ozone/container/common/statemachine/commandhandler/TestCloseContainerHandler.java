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

import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.ReplicationFactor;
import org.apache.hadoop.ozone.client.ReplicationType;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Test to behaviour of the datanode when recieve close container command.
 */
public class TestCloseContainerHandler {

  @Test
  public void test() throws IOException, TimeoutException, InterruptedException,
      OzoneException {

    //setup a cluster (1G free space is enough for a unit test)
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE_GB, "1");
    MiniOzoneClassicCluster cluster =
        new MiniOzoneClassicCluster.Builder(conf).numDataNodes(1)
            .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    cluster.waitOzoneReady();

    //the easiest way to create an open container is creating a key
    OzoneClient client = OzoneClientFactory.getClient(conf);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume("test");
    objectStore.getVolume("test").createBucket("test");
    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey("test", 1024, ReplicationType.STAND_ALONE,
            ReplicationFactor.ONE);
    key.write("test".getBytes());
    key.close();

    //get the name of a valid container
    KsmKeyArgs keyArgs =
        new KsmKeyArgs.Builder().setVolumeName("test").setBucketName("test")
            .setType(OzoneProtos.ReplicationType.STAND_ALONE)
            .setFactor(OzoneProtos.ReplicationFactor.ONE).setDataSize(1024)
            .setKeyName("test").build();

    KsmKeyLocationInfo ksmKeyLocationInfo =
        cluster.getKeySpaceManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    String containerName = ksmKeyLocationInfo.getContainerName();

    Assert.assertFalse(isContainerClosed(cluster, containerName));

    //send the order to close the container
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(cluster.getDataNodes().get(0).getDatanodeId(),
            new CloseContainerCommand(containerName));

    GenericTestUtils.waitFor(() -> isContainerClosed(cluster, containerName),
            500,
            5 * 1000);

    //double check if it's really closed (waitFor also throws an exception)
    Assert.assertTrue(isContainerClosed(cluster, containerName));
  }

  private Boolean isContainerClosed(MiniOzoneClassicCluster cluster,
      String containerName) {
    ContainerData containerData;
    try {
      containerData = cluster.getDataNodes().get(0).getOzoneContainerManager()
          .getContainerManager().readContainer(containerName);
      return !containerData.isOpen();
    } catch (StorageContainerException e) {
      throw new AssertionError(e);
    }
  }

}