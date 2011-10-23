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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Stores the plan for the move of an individual region.
 *
 * Contains info for the region being moved, info for the server the region
 * should be moved from, and info for the server the region should be moved
 * to.
 *
 * The comparable implementation of this class compares only the region
 * information and not the source/dest server info.
 */
public class RegionPlan implements Comparable<RegionPlan> {
  private final HRegionInfo hri;
  private final ServerName source;
  private ServerName dest;

  /**
   * Instantiate a plan for a region move, moving the specified region from
   * the specified source server to the specified destination server.
   *
   * Destination server can be instantiated as null and later set
   * with {@link #setDestination(ServerName)}.
   *
   * @param hri region to be moved
   * @param source regionserver region should be moved from
   * @param dest regionserver region should be moved to
   */
  public RegionPlan(final HRegionInfo hri, ServerName source, ServerName dest) {
    this.hri = hri;
    this.source = source;
    this.dest = dest;
  }

  /**
   * Set the destination server for the plan for this region.
   */
  public void setDestination(ServerName dest) {
    this.dest = dest;
  }

  /**
   * Get the source server for the plan for this region.
   * @return server info for source
   */
  public ServerName getSource() {
    return source;
  }

  /**
   * Get the destination server for the plan for this region.
   * @return server info for destination
   */
  public ServerName getDestination() {
    return dest;
  }

  /**
   * Get the encoded region name for the region this plan is for.
   * @return Encoded region name
   */
  public String getRegionName() {
    return this.hri.getEncodedName();
  }

  public HRegionInfo getRegionInfo() {
    return this.hri;
  }

  /**
   * Compare the region info.
   * @param o region plan you are comparing against
   */
  @Override
  public int compareTo(RegionPlan o) {
    return getRegionName().compareTo(o.getRegionName());
  }

  @Override
  public String toString() {
    return "hri=" + this.hri.getRegionNameAsString() + ", src=" +
      (this.source == null? "": this.source.toString()) +
      ", dest=" + (this.dest == null? "": this.dest.toString());
  }
}
