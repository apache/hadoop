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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Server;

/**
 * Interface to Map of online regions.  In the  Map, the key is the region's
 * encoded name and the value is an {@link HRegion} instance.
 */
interface OnlineRegions extends Server {
  /**
   * Add to online regions.
   * @param r
   */
  public void addToOnlineRegions(final HRegion r);

  /**
   * This method removes HRegion corresponding to hri from the Map of onlineRegions.
   *
   * @param encodedRegionName
   * @return True if we removed a region from online list.
   */
  public boolean removeFromOnlineRegions(String encodedRegionName);

  /**
   * Return {@link HRegion} instance.
   * Only works if caller is in same context, in same JVM. HRegion is not
   * serializable.
   * @param encodedRegionName
   * @return HRegion for the passed encoded <code>encodedRegionName</code> or
   * null if named region is not member of the online regions.
   */
  public HRegion getFromOnlineRegions(String encodedRegionName);
}