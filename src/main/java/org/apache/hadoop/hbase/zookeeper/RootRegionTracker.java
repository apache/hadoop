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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.RootLocationEditor;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tracks the root region server location node in zookeeper.
 * Root region location is set by {@link RootLocationEditor} usually called
 * out of <code>RegionServerServices</code>.
 * This class has a watcher on the root location and notices changes.
 */
public class RootRegionTracker extends ZooKeeperNodeTracker {
  /**
   * Creates a root region location tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   */
  public RootRegionTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher, watcher.rootServerZNode, abortable);
  }

  /**
   * Checks if the root region location is available.
   * @return true if root region location is available, false if not
   */
  public boolean isLocationAvailable() {
    return super.getData() != null;
  }

  /**
   * Gets the root region location, if available.  Null if not.  Does not block.
   * @return server name
   * @throws InterruptedException 
   */
  public ServerName getRootRegionLocation() throws InterruptedException {
    byte [] data = super.getData();
    return data == null? null: new ServerName(dataToString(data));
  }

  /**
   * Gets the root region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting root region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException if interrupted while waiting
   */
  public ServerName waitRootRegionLocation(long timeout)
  throws InterruptedException {
    String str = dataToString(super.blockUntilAvailable(timeout));
    return str == null? null: new ServerName(str);
  }

  /*
   * @param data
   * @return Returns null if <code>data</code> is null else converts passed data
   * to a String instance.
   */
  private static String dataToString(final byte [] data) {
    return data == null ? null: Bytes.toString(data);
  }
}