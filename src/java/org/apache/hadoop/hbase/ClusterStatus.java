/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.io.VersionedWritable;

/**
 * Status information on the HBase cluster.
 * <p>
 * <tt>ClusterStatus</tt> provides clients with information such as:
 * <ul>
 * <li>The count and names of region servers in the cluster.</li>
 * <li>The count and names of dead region servers in the cluster.</li>
 * <li>The average cluster load.</li>
 * <li>The number of regions deployed on the cluster.</li>
 * <li>The number of requests since last report.</li>
 * <li>Detailed region server loading and resource usage information,
 *  per server and per region.</li>
 * </ul>
 */
public class ClusterStatus extends VersionedWritable {
  private static final byte VERSION = 0;
  private Collection<HServerInfo> liveServerInfo;
  private Collection<String> deadServers;

  /**
   * Constructor, for Writable
   */
  public ClusterStatus() {
  }

  /**
   * @return the names of region servers in the cluster
   */
  public Collection<String> getServerNames() {
    ArrayList<String> names = new ArrayList<String>(liveServerInfo.size());
    for (HServerInfo server: liveServerInfo) {
      names.add(server.getName());
    }
    return names;
  }

  /**
   * @return the names of region servers on the dead list
   */
  public Collection<String> getDeadServerNames() {
    return Collections.unmodifiableCollection(deadServers);
  }

  /**
   * @return the number of region servers in the cluster
   */
  public int getServers() {
    return liveServerInfo.size();
  }

  /**
   * @return the number of dead region servers in the cluster
   */
  public int getDeadServers() {
    return deadServers.size();
  }

  /**
   * @return the average cluster load
   */
  public double getAverageLoad() {
    int load = 0;
    for (HServerInfo server: liveServerInfo) {
      load += server.getLoad().getLoad();
    }
    return (double)load / (double)liveServerInfo.size();
  }

  /**
   * @return the number of regions deployed on the cluster
   */
  public int getRegionsCount() {
    int count = 0;
    for (HServerInfo server: liveServerInfo) {
      count += server.getLoad().getNumberOfRegions();
    }
    return count;
  }

  /**
   * @return the number of requests since last report
   */
  public int getRequestsCount() {
    int count = 0;
    for (HServerInfo server: liveServerInfo) {
      count += server.getLoad().getNumberOfRequests();
    }
    return count;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusterStatus)) {
      return false;
    }
    return (getVersion() == ((ClusterStatus)o).getVersion()) &&
      liveServerInfo.equals(((ClusterStatus)o).liveServerInfo) &&
      deadServers.equals(((ClusterStatus)o).deadServers);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return VERSION + liveServerInfo.hashCode() + deadServers.hashCode();
  }

  /** @return the object version number */
  public byte getVersion() {
    return VERSION;
  }

  //
  // Getters
  //

  /**
   * Returns detailed region server information: A list of
   * {@link HServerInfo}, containing server load and resource usage
   * statistics as {@link HServerLoad}, containing per-region 
   * statistics as {@link HServerLoad.RegionLoad}.
   * @return region server information
   */
  public Collection<HServerInfo> getServerInfo() {
    return Collections.unmodifiableCollection(liveServerInfo);
  }

  //
  // Setters
  //

  public void setServerInfo(Collection<HServerInfo> serverInfo) {
    this.liveServerInfo = serverInfo;
  }

  public void setDeadServers(Collection<String> deadServers) {
    this.deadServers = deadServers;
  }

  //
  // Writable
  //

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(liveServerInfo.size());
    for (HServerInfo server: liveServerInfo) {
      server.write(out);
    }
    out.writeInt(deadServers.size());
    for (String server: deadServers) {
      out.writeUTF(server);
    }
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int count = in.readInt();
    liveServerInfo = new ArrayList<HServerInfo>(count);
    for (int i = 0; i < count; i++) {
      HServerInfo info = new HServerInfo();
      info.readFields(in);
      liveServerInfo.add(info);
    }
    count = in.readInt();
    deadServers = new ArrayList<String>(count);
    for (int i = 0; i < count; i++) {
      deadServers.add(in.readUTF());
    }
  }
}
