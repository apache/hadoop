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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the replication supervisor.
 */
public class TestReplicationSupervisor {

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Test
  public void normal() {
    //GIVEN
    ContainerSet set = new ContainerSet();

    FakeReplicator replicator = new FakeReplicator(set);
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(set, replicator, 5);

    List<DatanodeDetails> datanodes = IntStream.range(1, 3)
        .mapToObj(v -> Mockito.mock(DatanodeDetails.class))
        .collect(Collectors.toList());

    try {
      supervisor.start();
      //WHEN
      supervisor.addTask(new ReplicationTask(1L, datanodes));
      supervisor.addTask(new ReplicationTask(1L, datanodes));
      supervisor.addTask(new ReplicationTask(1L, datanodes));
      supervisor.addTask(new ReplicationTask(2L, datanodes));
      supervisor.addTask(new ReplicationTask(2L, datanodes));
      supervisor.addTask(new ReplicationTask(3L, datanodes));
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      //THEN
      System.out.println(replicator.replicated.get(0));

      Assert
          .assertEquals(3, replicator.replicated.size());

    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void duplicateMessageAfterAWhile() throws InterruptedException {
    //GIVEN
    ContainerSet set = new ContainerSet();

    FakeReplicator replicator = new FakeReplicator(set);
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(set, replicator, 2);

    List<DatanodeDetails> datanodes = IntStream.range(1, 3)
        .mapToObj(v -> Mockito.mock(DatanodeDetails.class))
        .collect(Collectors.toList());

    try {
      supervisor.start();
      //WHEN
      supervisor.addTask(new ReplicationTask(1L, datanodes));
      Thread.sleep(400);
      supervisor.addTask(new ReplicationTask(1L, datanodes));
      Thread.sleep(300);

      //THEN
      System.out.println(replicator.replicated.get(0));

      Assert
          .assertEquals(1, replicator.replicated.size());

      //the last item is still in the queue as we cleanup the queue during the
      // selection
      Assert.assertEquals(1, supervisor.getQueueSize());

    } finally {
      supervisor.stop();
    }
  }

  private class FakeReplicator implements ContainerReplicator {

    private List<ReplicationTask> replicated = new ArrayList<>();

    private ContainerSet containerSet;

    FakeReplicator(ContainerSet set) {
      this.containerSet = set;
    }

    @Override
    public void replicate(ReplicationTask task) {
      KeyValueContainerData kvcd =
          new KeyValueContainerData(task.getContainerId(), 100L);
      KeyValueContainer kvc =
          new KeyValueContainer(kvcd, conf);
      try {
        //download is slow
        Thread.sleep(100);
        replicated.add(task);
        containerSet.addContainer(kvc);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}