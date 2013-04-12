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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;

/**
 * Manage node decommissioning.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class DecommissionManager {
  static final Log LOG = LogFactory.getLog(DecommissionManager.class);

  private final Namesystem namesystem;
  private final BlockManager blockmanager;

  DecommissionManager(final Namesystem namesystem,
      final BlockManager blockmanager) {
    this.namesystem = namesystem;
    this.blockmanager = blockmanager;
  }

  /** Periodically check decommission status. */
  class Monitor implements Runnable {
    /** recheckInterval is how often namenode checks
     *  if a node has finished decommission
     */
    private final long recheckInterval;
    /** The number of decommission nodes to check for each interval */
    private final int numNodesPerCheck;
    /** firstkey can be initialized to anything. */
    private String firstkey = "";

    Monitor(int recheckIntervalInSecond, int numNodesPerCheck) {
      this.recheckInterval = recheckIntervalInSecond * 1000L;
      this.numNodesPerCheck = numNodesPerCheck;
    }

    /**
     * Check decommission status of numNodesPerCheck nodes
     * for every recheckInterval milliseconds.
     */
    @Override
    public void run() {
      for(; namesystem.isRunning(); ) {
        namesystem.writeLock();
        try {
          check();
        } finally {
          namesystem.writeUnlock();
        }
  
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn(this.getClass().getSimpleName() + " interrupted: " + ie);
        }
      }
    }
    
    private void check() {
      final DatanodeManager dm = blockmanager.getDatanodeManager();
      int count = 0;
      for(Map.Entry<String, DatanodeDescriptor> entry
          : dm.getDatanodeCyclicIteration(firstkey)) {
        final DatanodeDescriptor d = entry.getValue();
        firstkey = entry.getKey();

        if (d.isDecommissionInProgress()) {
          try {
            dm.checkDecommissionState(d);
          } catch(Exception e) {
            LOG.warn("entry=" + entry, e);
          }
          if (++count == numNodesPerCheck) {
            return;
          }
        }
      }
    }
  }
}
