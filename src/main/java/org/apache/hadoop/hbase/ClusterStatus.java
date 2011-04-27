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
import java.util.HashMap;
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
   *   <dt>2</dt> <dd>Added Map of ServerName to ServerLoad</dd>
   * </dl>
   */
  private static final byte VERSION = 2;

  private String hbaseVersion;
  private Map<ServerName, HServerLoad> liveServers;
  private Collection<ServerName> deadServers;
  private Map<String, RegionState> intransition;
  private String clusterId;

  /**
   * Constructor, for Writable
   */
  public ClusterStatus() {
    super();
  }

  public ClusterStatus(final String hbaseVersion, final String clusterid,
      final Map<ServerName, HServerLoad> servers,
      final Collection<ServerName> deadServers, final Map<String, RegionState> rit) {
    this.hbaseVersion = hbaseVersion;
    this.liveServers = servers;
    this.deadServers = deadServers;
    this.intransition = rit;
    this.clusterId = clusterid;
  }

  /**
   * @return the names of region servers on the dead list
   */
  public Collection<ServerName> getDeadServerNames() {
    return Collections.unmodifiableCollection(deadServers);
  }

  /**
   * @return the number of region servers in the cluster
   */
  public int getServersSize() {
    return liveServers.size();
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
    int load = getRegionsCount();
    return (double)load / (double)getServersSize();
  }

  /**
   * @return the number of regions deployed on the cluster
   */
  public int getRegionsCount() {
    int count = 0;
    for (Map.Entry<ServerName, HServerLoad> e: this.liveServers.entrySet()) {
      count += e.getValue().getNumberOfRegions();
    }
    return count;
  }

  /**
   * @return the number of requests since last report
   */
  public int getRequestsCount() {
    int count = 0;
    for (Map.Entry<ServerName, HServerLoad> e: this.liveServers.entrySet()) {
      count += e.getValue().getNumberOfRequests();
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
      this.liveServers.equals(((ClusterStatus)o).liveServers) &&
      deadServers.equals(((ClusterStatus)o).deadServers);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return VERSION + hbaseVersion.hashCode() + this.liveServers.hashCode() +
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
   * {@link ServerName}.
   * @return region server information
   * @deprecated Use {@link #getServers()}
   */
  public Collection<ServerName> getServerInfo() {
    return getServers();
  }

  public Collection<ServerName> getServers() {
    return Collections.unmodifiableCollection(this.liveServers.keySet());
  }

  /**
   * @param sn
   * @return Server's load or null if not found.
   */
  public HServerLoad getLoad(final ServerName sn) {
    return this.liveServers.get(sn);
  }

  public Map<String, RegionState> getRegionsInTransition() {
    return this.intransition;
  }

  public String getClusterId() {
    return clusterId;
  }

  //
  // Writable
  //

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(hbaseVersion);
    out.writeInt(getServersSize());
    for (Map.Entry<ServerName, HServerLoad> e: this.liveServers.entrySet()) {
      out.writeUTF(e.getKey().toString());
      e.getValue().write(out);
    }
    out.writeInt(deadServers.size());
    for (ServerName server: deadServers) {
      out.writeUTF(server.toString());
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
    this.liveServers = new HashMap<ServerName, HServerLoad>(count);
    for (int i = 0; i < count; i++) {
      String str = in.readUTF();
      HServerLoad hsl = new HServerLoad();
      hsl.readFields(in);
      this.liveServers.put(new ServerName(str), hsl);
    }
    count = in.readInt();
    deadServers = new ArrayList<ServerName>(count);
    for (int i = 0; i < count; i++) {
      deadServers.add(new ServerName(in.readUTF()));
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