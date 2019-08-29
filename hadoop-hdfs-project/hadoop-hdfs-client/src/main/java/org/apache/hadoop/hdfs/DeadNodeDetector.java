/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detect the dead nodes in advance, and share this information among all the
 * DFSInputStreams in the same client.
 */
public class DeadNodeDetector implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(DeadNodeDetector.class);

  private static final long ERROR_SLEEP_MS = 5000;
  private static final long IDLE_SLEEP_MS = 10000;

  private String name;

  /**
   * Dead nodes shared by all the DFSInputStreams of the client.
   */
  private final ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes;

  /**
   * Record dead nodes by one DFSInputStream. When dead node is not used by one
   * DFSInputStream, remove it from dfsInputStreamNodes#DFSInputStream. If
   * DFSInputStream does not include any dead node, remove DFSInputStream from
   * dfsInputStreamNodes.
   */
  private final ConcurrentHashMap<DFSInputStream, HashSet<DatanodeInfo>>
          dfsInputStreamNodes;

  /**
   * The state of DeadNodeDetector.
   */
  private enum State {
    INIT, CHECK_DEAD, IDLE, ERROR
  }

  private State state;

  public DeadNodeDetector(String name) {
    this.deadNodes = new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();
    this.dfsInputStreamNodes =
        new ConcurrentHashMap<DFSInputStream, HashSet<DatanodeInfo>>();
    this.name = name;

    LOG.info("start dead node detector for DFSClient " + this.name);
    state = State.INIT;
  }

  @Override
  public void run() {
    while (true) {
      LOG.debug("state " + state);
      switch (state) {
      case INIT:
        init();
        break;
      case IDLE:
        idle();
        break;
      case ERROR:
        try {
          Thread.sleep(ERROR_SLEEP_MS);
        } catch (InterruptedException e) {
        }
        return;
      default:
        break;
      }
    }
  }

  private void idle() {
    try {
      Thread.sleep(IDLE_SLEEP_MS);
    } catch (InterruptedException e) {

    }

    state = State.IDLE;
  }

  private void init() {
    state = State.IDLE;
  }

  private void addToDead(DatanodeInfo datanodeInfo) {
    deadNodes.put(datanodeInfo, datanodeInfo);
  }

  public boolean isDeadNode(DatanodeInfo datanodeInfo) {
    return deadNodes.contains(datanodeInfo);
  }

  /**
   * Add datanode in deadNodes and dfsInputStreamNodes. The node is considered
   * to dead node. The dead node is shared by all the DFSInputStreams in the
   * same client.
   */
  public synchronized void addNodeToDetect(DFSInputStream dfsInputStream,
      DatanodeInfo datanodeInfo) {
    HashSet<DatanodeInfo> datanodeInfos =
        dfsInputStreamNodes.get(dfsInputStream);
    if (datanodeInfos == null) {
      datanodeInfos = new HashSet<DatanodeInfo>();
      datanodeInfos.add(datanodeInfo);
      dfsInputStreamNodes.putIfAbsent(dfsInputStream, datanodeInfos);
    } else {
      datanodeInfos.add(datanodeInfo);
    }

    addToDead(datanodeInfo);
  }

  /**
   * Remove dead node which is not used by any DFSInputStream from deadNodes.
   * @return new dead node shared by all DFSInputStreams.
   */
  public synchronized Set<DatanodeInfo> getDeadNodesToDetect() {
    // remove the dead nodes who doesn't have any inputstream first
    Set<DatanodeInfo> newDeadNodes = new HashSet<DatanodeInfo>();
    for (HashSet<DatanodeInfo> datanodeInfos : dfsInputStreamNodes.values()) {
      newDeadNodes.addAll(datanodeInfos);
    }

    newDeadNodes.retainAll(deadNodes.values());

    for (DatanodeInfo datanodeInfo : deadNodes.values()) {
      if (!newDeadNodes.contains(datanodeInfo)) {
        deadNodes.remove(datanodeInfo);
      }
    }
    return newDeadNodes;
  }

  /**
   * Remove dead node from dfsInputStreamNodes#dfsInputStream. If
   * dfsInputStreamNodes#dfsInputStream does not contain any dead node, remove
   * it from dfsInputStreamNodes.
   */
  public synchronized void removeNodeFromDetectByDFSInputStream(
      DFSInputStream dfsInputStream, DatanodeInfo datanodeInfo) {
    Set<DatanodeInfo> datanodeInfos = dfsInputStreamNodes.get(dfsInputStream);
    if (datanodeInfos != null) {
      datanodeInfos.remove(datanodeInfo);
      if (datanodeInfos.isEmpty()) {
        dfsInputStreamNodes.remove(dfsInputStream);
      }
    }
  }
}
