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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single point to schedule the downloading tasks based on priorities.
 */
public class ReplicationSupervisor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationSupervisor.class);

  private final Set<Worker> threadPool = new HashSet<>();

  private final Map<Long, ReplicationTask> queue = new TreeMap();

  private final ContainerSet containerSet;

  private final ContainerReplicator replicator;

  private final int poolSize;

  public ReplicationSupervisor(
      ContainerSet containerSet,
      ContainerReplicator replicator, int poolSize) {
    this.containerSet = containerSet;
    this.replicator = replicator;
    this.poolSize = poolSize;
  }

  public synchronized void addTask(ReplicationTask task) {
    queue.putIfAbsent(task.getContainerId(), task);
    synchronized (threadPool) {
      threadPool.notify();
    }
  }

  public void start() {
    for (int i = 0; i < poolSize; i++) {
      Worker worker = new Worker();
      Thread thread = new Thread(worker, "ContainerReplication-" + i);
      thread.setDaemon(true);
      thread.start();
      threadPool.add(worker);
    }
  }

  public synchronized ReplicationTask selectTask() {
    for (ReplicationTask task : queue.values()) {
      if (task.getStatus() == Status.QUEUED) {
        if (containerSet.getContainer(task.getContainerId()) == null) {
          task.setStatus(Status.DOWNLOADING);
          return task;
        } else {
          LOG.debug("Container {} has already been downloaded.",
              task.getContainerId());
          queue.remove(task.getContainerId());
        }
      } else if (task.getStatus() == Status.FAILED) {
        LOG.error(
            "Container {} can't be downloaded from any of the datanodes.",
            task.getContainerId());
        queue.remove(task.getContainerId());
      } else if (task.getStatus() == Status.DONE) {
        queue.remove(task.getContainerId());
        LOG.info("Container {} is replicated.", task.getContainerId());
      }
    }
    //no available task.
    return null;
  }

  public void stop() {
    for (Worker worker : threadPool) {
      worker.stop();
    }
  }

  @VisibleForTesting
  public int getQueueSize() {
    return queue.size();
  }

  private class Worker implements Runnable {

    private boolean running = true;

    @Override
    public void run() {
      try {
        while (running) {
          ReplicationTask task = selectTask();
          if (task == null) {
            synchronized (threadPool) {
              threadPool.wait();
            }
          } else {
            replicator.replicate(task);
          }
        }
      } catch (Exception ex) {
        LOG.error("Error on doing replication", ex);
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          LOG.error("Error on waiting after failed replication task", e);
        }
      }
    }

    public void stop() {
      running = false;
    }
  }
}
