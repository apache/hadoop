/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.io.Text;

/**
 * Used as a callback mechanism so that an HRegion can notify the HRegionServer
 * of the different stages making an HRegion unavailable.  Regions are made
 * unavailable during region split operations.
 */
public interface RegionUnavailableListener {
  /**
   * <code>regionName</code> is closing.
   * Listener should stop accepting new writes but can continue to service
   * outstanding transactions.
   * @param regionName
   */
  public void closing(final Text regionName);
  
  /**
   * <code>regionName</code> is closed and no longer available.
   * Listener should clean up any references to <code>regionName</code>
   * @param regionName
   */
  public void closed(final Text regionName);
}
