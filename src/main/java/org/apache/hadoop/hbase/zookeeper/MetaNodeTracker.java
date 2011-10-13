/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * Tracks the unassigned zookeeper node used by the META table.
 * <p>
 * If META is already assigned when instantiating this class, you will not
 * receive any notification for that assignment.  You will receive a
 * notification after META has been successfully assigned to a new location.
 */
public class MetaNodeTracker extends ZooKeeperNodeTracker {
  /**
   * Creates a meta node tracker.
   * @param watcher
   * @param abortable
   */
  public MetaNodeTracker(final ZooKeeperWatcher watcher, final Abortable abortable) {
    super(watcher, ZKUtil.joinZNode(watcher.assignmentZNode,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName()), abortable);
  }

  @Override
  public void nodeDeleted(String path) {
    super.nodeDeleted(path);
  }
}