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

import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import java.util.Queue;

/**
 * Interface used to implement a decommission and maintenance monitor class,
 * which is instantiated by the DatanodeAdminManager class.
 */

public interface DatanodeAdminMonitorInterface extends Runnable {
  void stopTrackingNode(DatanodeDescriptor dn);
  void startTrackingNode(DatanodeDescriptor dn);
  int getPendingNodeCount();
  int getTrackedNodeCount();
  int getNumNodesChecked();
  Queue<DatanodeDescriptor> getPendingNodes();

  void setBlockManager(BlockManager bm);
  void setDatanodeAdminManager(DatanodeAdminManager dnm);
  void setNameSystem(Namesystem ns);
}