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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

/**
 * Provides base implementation of NodeLabelsProvider with Timer and expects
 * subclass to provide TimerTask which can fetch NodeLabels
 */
public abstract class AbstractNodeLabelsProvider extends AbstractService
    implements NodeLabelsProvider {
  public static final long DISABLE_NODE_LABELS_PROVIDER_FETCH_TIMER = -1;

  // Delay after which timer task are triggered to fetch NodeLabels
  protected long intervalTime;

  // Timer used to schedule node labels fetching
  protected Timer nodeLabelsScheduler;

  public static final String NODE_LABELS_SEPRATOR = ",";

  protected Lock readLock = null;
  protected Lock writeLock = null;

  protected TimerTask timerTask;

  protected Set<NodeLabel> nodeLabels =
      CommonNodeLabelsManager.EMPTY_NODELABEL_SET;


  public AbstractNodeLabelsProvider(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.intervalTime =
        conf.getLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS);

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    timerTask = createTimerTask();
    timerTask.run();
    if (intervalTime != DISABLE_NODE_LABELS_PROVIDER_FETCH_TIMER) {
      nodeLabelsScheduler =
          new Timer("DistributedNodeLabelsRunner-Timer", true);
      // Start the timer task and then periodically at the configured interval
      // time. Illegal values for intervalTime is handled by timer api
      nodeLabelsScheduler.scheduleAtFixedRate(timerTask, intervalTime,
          intervalTime);
    }
    super.serviceStart();
  }

  /**
   * terminate the timer
   * @throws Exception
   */
  @Override
  protected void serviceStop() throws Exception {
    if (nodeLabelsScheduler != null) {
      nodeLabelsScheduler.cancel();
    }
    cleanUp();
    super.serviceStop();
  }

  /**
   * method for subclasses to cleanup.
   */
  protected abstract void cleanUp() throws Exception ;

  /**
   * @return Returns output from provider.
   */
  @Override
  public Set<NodeLabel> getNodeLabels() {
    readLock.lock();
    try {
      return nodeLabels;
    } finally {
      readLock.unlock();
    }
  }

  protected void setNodeLabels(Set<NodeLabel> nodeLabelsSet) {
    writeLock.lock();
    try {
      nodeLabels = nodeLabelsSet;
    } finally {
      writeLock.unlock();
    }
  }

  static Set<NodeLabel> convertToNodeLabelSet(String partitionNodeLabel) {
    if (null == partitionNodeLabel) {
      return null;
    }
    Set<NodeLabel> labels = new HashSet<NodeLabel>();
    labels.add(NodeLabel.newInstance(partitionNodeLabel));
    return labels;
  }

  /**
   * Used only by tests to access the timer task directly
   *
   * @return the timer task
   */
  TimerTask getTimerTask() {
    return timerTask;
  }

  public abstract TimerTask createTimerTask();
}
