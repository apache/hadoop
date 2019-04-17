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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringInterner;

/**
 * Represents the network location of a block, information about the hosts
 * that contain block replicas, and other block metadata (E.g. the file
 * offset associated with the block, length, whether it is corrupt, etc).
 *
 * For a single BlockLocation, it will have different meanings for replicated
 * and erasure coded files.
 *
 * If the file is 3-replicated, offset and length of a BlockLocation represent
 * the absolute value in the file and the hosts are the 3 datanodes that
 * holding the replicas. Here is an example:
 * <pre>
 * BlockLocation(offset: 0, length: BLOCK_SIZE,
 *   hosts: {"host1:9866", "host2:9866, host3:9866"})
 * </pre>
 *
 * And if the file is erasure-coded, each BlockLocation represents a logical
 * block groups. Value offset is the offset of a block group in the file and
 * value length is the total length of a block group. Hosts of a BlockLocation
 * are the datanodes that holding all the data blocks and parity blocks of a
 * block group.
 * Suppose we have a RS_3_2 coded file (3 data units and 2 parity units).
 * A BlockLocation example will be like:
 * <pre>
 * BlockLocation(offset: 0, length: 3 * BLOCK_SIZE, hosts: {"host1:9866",
 *   "host2:9866","host3:9866","host4:9866","host5:9866"})
 * </pre>
 *
 * Please refer to
 * {@link FileSystem#getFileBlockLocations(FileStatus, long, long)} or
 * {@link FileContext#getFileBlockLocations(Path, long, long)}
 * for more examples.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BlockLocation implements Serializable {
  private static final long serialVersionUID = 0x22986f6d;

  private String[] hosts; // Datanode hostnames
  private String[] cachedHosts; // Datanode hostnames with a cached replica
  private String[] names; // Datanode IP:xferPort for accessing the block
  private String[] topologyPaths; // Full path name in network topology
  private String[] storageIds; // Storage ID of each replica
  private StorageType[] storageTypes; // Storage type of each replica
  private long offset;  // Offset of the block in the file
  private long length;
  private boolean corrupt;

  private static final String[] EMPTY_STR_ARRAY = new String[0];
  private static final StorageType[] EMPTY_STORAGE_TYPE_ARRAY =
      new StorageType[0];

  /**
   * Default Constructor.
   */
  public BlockLocation() {
    this(EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, 0L, 0L);
  }

  /**
   * Copy constructor.
   */
  public BlockLocation(BlockLocation that) {
    this.hosts = that.hosts;
    this.cachedHosts = that.cachedHosts;
    this.names = that.names;
    this.topologyPaths = that.topologyPaths;
    this.offset = that.offset;
    this.length = that.length;
    this.corrupt = that.corrupt;
    this.storageIds = that.storageIds;
    this.storageTypes = that.storageTypes;
  }

  /**
   * Constructor with host, name, offset and length.
   */
  public BlockLocation(String[] names, String[] hosts, long offset, 
                       long length) {
    this(names, hosts, offset, length, false);
  }

  /**
   * Constructor with host, name, offset, length and corrupt flag.
   */
  public BlockLocation(String[] names, String[] hosts, long offset, 
                       long length, boolean corrupt) {
    this(names, hosts, null, offset, length, corrupt);
  }

  /**
   * Constructor with host, name, network topology, offset and length.
   */
  public BlockLocation(String[] names, String[] hosts, String[] topologyPaths,
                       long offset, long length) {
    this(names, hosts, topologyPaths, offset, length, false);
  }

  /**
   * Constructor with host, name, network topology, offset, length 
   * and corrupt flag.
   */
  public BlockLocation(String[] names, String[] hosts, String[] topologyPaths,
                       long offset, long length, boolean corrupt) {
    this(names, hosts, null, topologyPaths, offset, length, corrupt);
  }

  public BlockLocation(String[] names, String[] hosts, String[] cachedHosts,
      String[] topologyPaths, long offset, long length, boolean corrupt) {
    this(names, hosts, cachedHosts, topologyPaths, null, null, offset, length,
        corrupt);
  }

  public BlockLocation(String[] names, String[] hosts, String[] cachedHosts,
      String[] topologyPaths, String[] storageIds, StorageType[] storageTypes,
      long offset, long length, boolean corrupt) {
    if (names == null) {
      this.names = EMPTY_STR_ARRAY;
    } else {
      this.names = StringInterner.internStringsInArray(names);
    }
    if (hosts == null) {
      this.hosts = EMPTY_STR_ARRAY;
    } else {
      this.hosts = StringInterner.internStringsInArray(hosts);
    }
    if (cachedHosts == null) {
      this.cachedHosts = EMPTY_STR_ARRAY;
    } else {
      this.cachedHosts = StringInterner.internStringsInArray(cachedHosts);
    }
    if (topologyPaths == null) {
      this.topologyPaths = EMPTY_STR_ARRAY;
    } else {
      this.topologyPaths = StringInterner.internStringsInArray(topologyPaths);
    }
    if (storageIds == null) {
      this.storageIds = EMPTY_STR_ARRAY;
    } else {
      this.storageIds = StringInterner.internStringsInArray(storageIds);
    }
    if (storageTypes == null) {
      this.storageTypes = EMPTY_STORAGE_TYPE_ARRAY;
    } else {
      this.storageTypes = storageTypes;
    }
    this.offset = offset;
    this.length = length;
    this.corrupt = corrupt;
  }

  /**
   * Get the list of hosts (hostname) hosting this block.
   */
  public String[] getHosts() throws IOException {
    return hosts;
  }

  /**
   * Get the list of hosts (hostname) hosting a cached replica of the block.
   */
  public String[] getCachedHosts() {
    return cachedHosts;
  }

  /**
   * Get the list of names (IP:xferPort) hosting this block.
   */
  public String[] getNames() throws IOException {
    return names;
  }

  /**
   * Get the list of network topology paths for each of the hosts.
   * The last component of the path is the "name" (IP:xferPort).
   */
  public String[] getTopologyPaths() throws IOException {
    return topologyPaths;
  }

  /**
   * Get the storageID of each replica of the block.
   */
  public String[] getStorageIds() {
    return storageIds;
  }

  /**
   * Get the storage type of each replica of the block.
   */
  public StorageType[] getStorageTypes() {
    return storageTypes;
  }

  /**
   * Get the start offset of file associated with this block.
   */
  public long getOffset() {
    return offset;
  }
  
  /**
   * Get the length of the block.
   */
  public long getLength() {
    return length;
  }

  /**
   * Get the corrupt flag.
   */
  public boolean isCorrupt() {
    return corrupt;
  }

  /**
   * Return true if the block is striped (erasure coded).
   */
  public boolean isStriped() {
    return false;
  }

  /**
   * Set the start offset of file associated with this block.
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Set the length of block.
   */
  public void setLength(long length) {
    this.length = length;
  }

  /**
   * Set the corrupt flag.
   */
  public void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  /**
   * Set the hosts hosting this block.
   */
  public void setHosts(String[] hosts) throws IOException {
    if (hosts == null) {
      this.hosts = EMPTY_STR_ARRAY;
    } else {
      this.hosts = StringInterner.internStringsInArray(hosts);
    }
  }

  /**
   * Set the hosts hosting a cached replica of this block.
   */
  public void setCachedHosts(String[] cachedHosts) {
    if (cachedHosts == null) {
      this.cachedHosts = EMPTY_STR_ARRAY;
    } else {
      this.cachedHosts = StringInterner.internStringsInArray(cachedHosts);
    }
  }

  /**
   * Set the names (host:port) hosting this block.
   */
  public void setNames(String[] names) throws IOException {
    if (names == null) {
      this.names = EMPTY_STR_ARRAY;
    } else {
      this.names = StringInterner.internStringsInArray(names);
    }
  }

  /**
   * Set the network topology paths of the hosts.
   */
  public void setTopologyPaths(String[] topologyPaths) throws IOException {
    if (topologyPaths == null) {
      this.topologyPaths = EMPTY_STR_ARRAY;
    } else {
      this.topologyPaths = StringInterner.internStringsInArray(topologyPaths);
    }
  }

  public void setStorageIds(String[] storageIds) {
    if (storageIds == null) {
      this.storageIds = EMPTY_STR_ARRAY;
    } else {
      this.storageIds = StringInterner.internStringsInArray(storageIds);
    }
  }

  public void setStorageTypes(StorageType[] storageTypes) {
    if (storageTypes == null) {
      this.storageTypes = EMPTY_STORAGE_TYPE_ARRAY;
    } else {
      this.storageTypes = storageTypes;
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(offset)
        .append(',')
        .append(length);
    if (corrupt) {
      result.append("(corrupt)");
    }
    for(String h: hosts) {
      result.append(',');
      result.append(h);
    }
    return result.toString();
  }
}
