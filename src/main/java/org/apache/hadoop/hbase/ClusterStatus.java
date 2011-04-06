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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
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
 *  <li>Regions in transition at master</li>
 *  <li>The unique cluster ID</li>
 * </ul>
 */
public class ClusterStatus extends VersionedWritable {
  /**
   * Version for object serialization.  Incremented for changes in serialized
   * representation.
   * <dl>
   *   <dt>0</dt> <dd>initial version</dd>
   *   <dt>1</dt> <dd>added cluster ID</dd>
   * </dl>
   */
  private static final byte VERSION = 1;

  private String hbaseVersion;
  private Collection<HServerInfo> liveServerInfo;
  private Collection<String> deadServers;
  private Map<String, RegionState> intransition;
  private String clusterId;

  /**
   * Constructor, for Writable
   */
  public ClusterStatus() {
    super();
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
   * @return the HBase version string as reported by the HMaster
   */
  public String getHBaseVersion() {
    return hbaseVersion;
  }

  /**
   * @param version the HBase version string
   */
  public void setHBaseVersion(String version) {
    hbaseVersion = version;
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
      getHBaseVersion().equals(((ClusterStatus)o).getHBaseVersion()) &&
      liveServerInfo.equals(((ClusterStatus)o).liveServerInfo) &&
      deadServers.equals(((ClusterStatus)o).deadServers);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return VERSION + hbaseVersion.hashCode() + liveServerInfo.hashCode() +
      deadServers.hashCode();
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

  public Map<String, RegionState> getRegionsInTransition() {
    return this.intransition;
  }

  public void setRegionsInTransition(final Map<String, RegionState> m) {
    this.intransition = m;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String id) {
    this.clusterId = id;
  }

  //
  // Writable
  //

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(hbaseVersion);
    out.writeInt(liveServerInfo.size());
    for (HServerInfo server: liveServerInfo) {
      server.write(out);
    }
    out.writeInt(deadServers.size());
    for (String server: deadServers) {
      out.writeUTF(server);
    }
    out.writeInt(this.intransition.size());
    for (Map.Entry<String, RegionState> e: this.intransition.entrySet()) {
      out.writeUTF(e.getKey());
      e.getValue().write(out);
    }
    out.writeUTF(clusterId);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    hbaseVersion = in.readUTF();
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
    count = in.readInt();
    this.intransition = new TreeMap<String, RegionState>();
    for (int i = 0; i < count; i++) {
      String key = in.readUTF();
      RegionState regionState = new RegionState();
      regionState.readFields(in);
      this.intransition.put(key, regionState);
    }
    this.clusterId = in.readUTF();
  }
}
