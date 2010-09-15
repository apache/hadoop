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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.catalog.CatalogTracker;

/**
 * Tracks the unassigned zookeeper node used by the META table.
 *
 * A callback is made into the passed {@link CatalogTracker} when
 * <code>.META.</code> completes a new assignment.
 * <p>
 * If META is already assigned when instantiating this class, you will not
 * receive any notification for that assignment.  You will receive a
 * notification after META has been successfully assigned to a new location.
 */
public class MetaNodeTracker extends ZooKeeperNodeTracker {
  private static final Log LOG = LogFactory.getLog(MetaNodeTracker.class);

  /** Catalog tracker to notify when META has a new assignment completed. */
  private final CatalogTracker catalogTracker;

  /**
   * Creates a meta node tracker.
   * @param watcher
   * @param abortable
   */
  public MetaNodeTracker(final ZooKeeperWatcher watcher,
      final CatalogTracker catalogTracker, final Abortable abortable) {
    super(watcher, ZKUtil.joinZNode(watcher.assignmentZNode,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName()), abortable);
    this.catalogTracker = catalogTracker;
  }

  @Override
  public void nodeDeleted(String path) {
    if (!path.equals(node)) return;
    LOG.info("Detected completed assignment of META, notifying catalog tracker");
    try {
      this.catalogTracker.waitForMetaServerConnectionDefault();
    } catch (IOException e) {
      LOG.warn("Tried to reset META server location after seeing the " +
        "completion of a new META assignment but got an IOE", e);
    }
  }
}