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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.VersionedWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class is used exporting current state of load on a RegionServer.
 */
public class HServerLoad extends VersionedWritable
implements WritableComparable<HServerLoad> {
  private static final byte VERSION = 2;
  // Empty load instance.
  public static final HServerLoad EMPTY_HSERVERLOAD = new HServerLoad();

  /** Number of requests since last report
   */
  // TODO: Instead build this up out of region counters.
  private int numberOfRequests = 0;

  /** the amount of used heap, in MB */
  private int usedHeapMB = 0;

  /** the maximum allowable size of the heap, in MB */
  private int maxHeapMB = 0;

  /** per-region load metrics */
  private Map<byte[], RegionLoad> regionLoad =
    new TreeMap<byte[], RegionLoad>(Bytes.BYTES_COMPARATOR);

  /** @return the object version number */
  public byte getVersion() {
    return VERSION;
  }

  /**
   * Encapsulates per-region loading metrics.
   */
  public static class RegionLoad extends VersionedWritable {
    private static final byte VERSION = 1;

    /** @return the object version number */
    public byte getVersion() {
      return VERSION;
    }

    /** the region name */
    private byte[] name;
    /** the number of stores for the region */
    private int stores;
    /** the number of storefiles for the region */
    private int storefiles;
    /** the total size of the store files for the region, uncompressed, in MB */
    private int storeUncompressedSizeMB;
    /** the current total size of the store files for the region, in MB */
    private int storefileSizeMB;
    /** the current size of the memstore for the region, in MB */
    private int memstoreSizeMB;
    /** the current total size of storefile indexes for the region, in MB */
    private int storefileIndexSizeMB;
    /** the current total read requests made to region */
    private int readRequestsCount;
    /** the current total write requests made to region */
    private int writeRequestsCount;

    /**
     * Constructor, for Writable
     */
    public RegionLoad() {
        super();
    }

    /**
     * @param name
     * @param stores
     * @param storefiles
     * @param storefileSizeMB
     * @param memstoreSizeMB
     * @param storefileIndexSizeMB
     * @param readRequestsCount
     * @param writeRequestsCount
     */
    public RegionLoad(final byte[] name, final int stores,
        final int storefiles, final int storeUncompressedSizeMB,
        final int storefileSizeMB,
        final int memstoreSizeMB, final int storefileIndexSizeMB,
        final int readRequestsCount, final int writeRequestsCount) {
      this.name = name;
      this.stores = stores;
      this.storefiles = storefiles;
      this.storeUncompressedSizeMB = storeUncompressedSizeMB;
      this.storefileSizeMB = storefileSizeMB;
      this.memstoreSizeMB = memstoreSizeMB;
      this.storefileIndexSizeMB = storefileIndexSizeMB;
      this.readRequestsCount = readRequestsCount;
      this.writeRequestsCount = writeRequestsCount;
    }

    // Getters

    /**
     * @return the region name
     */
    public byte[] getName() {
      return name;
    }

    /**
     * @return the region name as a string
     */
    public String getNameAsString() {
      return Bytes.toString(name);
    }

    /**
     * @return the number of stores
     */
    public int getStores() {
      return stores;
    }

    /**
     * @return the number of storefiles
     */
    public int getStorefiles() {
      return storefiles;
    }

    /**
     * @return the total size of the storefiles, in MB
     */
    public int getStorefileSizeMB() {
      return storefileSizeMB;
    }

    /**
     * @return the memstore size, in MB
     */
    public int getMemStoreSizeMB() {
      return memstoreSizeMB;
    }

    /**
     * @return the approximate size of storefile indexes on the heap, in MB
     */
    public int getStorefileIndexSizeMB() {
      return storefileIndexSizeMB;
    }
    
    /**
     * @return the number of requests made to region
     */
    public long getRequestsCount() {
      return readRequestsCount + writeRequestsCount;
    }

    /**
     * @return the number of read requests made to region
     */
    public long getReadRequestsCount() {
      return readRequestsCount;
    }

    /**
     * @return the number of read requests made to region
     */
    public long getWriteRequestsCount() {
      return writeRequestsCount;
    }

    // Setters

    /**
     * @param name the region name
     */
    public void setName(byte[] name) {
      this.name = name;
    }

    /**
     * @param stores the number of stores
     */
    public void setStores(int stores) {
      this.stores = stores;
    }

    /**
     * @param storefiles the number of storefiles
     */
    public void setStorefiles(int storefiles) {
      this.storefiles = storefiles;
    }

    /**
     * @param memstoreSizeMB the memstore size, in MB
     */
    public void setMemStoreSizeMB(int memstoreSizeMB) {
      this.memstoreSizeMB = memstoreSizeMB;
    }

    /**
     * @param storefileIndexSizeMB the approximate size of storefile indexes
     *  on the heap, in MB
     */
    public void setStorefileIndexSizeMB(int storefileIndexSizeMB) {
      this.storefileIndexSizeMB = storefileIndexSizeMB;
    }

    /**
     * @param requestsCount the number of read requests to region
     */
    public void setReadRequestsCount(int requestsCount) {
      this.readRequestsCount = requestsCount;
    }

    /**
     * @param requestsCount the number of write requests to region
     */
    public void setWriteRequestsCount(int requestsCount) {
      this.writeRequestsCount = requestsCount;
    }

    // Writable
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      int version = in.readByte();
      if (version > VERSION) throw new IOException("Version mismatch; " + version);
      int namelen = in.readInt();
      this.name = new byte[namelen];
      in.readFully(this.name);
      this.stores = in.readInt();
      this.storefiles = in.readInt();
      this.storeUncompressedSizeMB = in.readInt();
      this.storefileSizeMB = in.readInt();
      this.memstoreSizeMB = in.readInt();
      this.storefileIndexSizeMB = in.readInt();
      this.readRequestsCount = in.readInt();
      this.writeRequestsCount = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeByte(VERSION);
      out.writeInt(name.length);
      out.write(name);
      out.writeInt(stores);
      out.writeInt(storefiles);
      out.writeInt(storeUncompressedSizeMB);
      out.writeInt(storefileSizeMB);
      out.writeInt(memstoreSizeMB);
      out.writeInt(storefileIndexSizeMB);
      out.writeInt(readRequestsCount);
      out.writeInt(writeRequestsCount);
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "stores",
        Integer.valueOf(this.stores));
      sb = Strings.appendKeyValue(sb, "storefiles",
        Integer.valueOf(this.storefiles));
      sb = Strings.appendKeyValue(sb, "storefileUncompressedSizeMB",
        Integer.valueOf(this.storeUncompressedSizeMB));
      sb = Strings.appendKeyValue(sb, "storefileSizeMB",
          Integer.valueOf(this.storefileSizeMB));
      if (this.storeUncompressedSizeMB != 0) {
        sb = Strings.appendKeyValue(sb, "compressionRatio",
            String.format("%.4f", (float)this.storefileSizeMB/
                (float)this.storeUncompressedSizeMB));        
      }
      sb = Strings.appendKeyValue(sb, "memstoreSizeMB",
        Integer.valueOf(this.memstoreSizeMB));
      sb = Strings.appendKeyValue(sb, "storefileIndexSizeMB",
        Integer.valueOf(this.storefileIndexSizeMB));
      sb = Strings.appendKeyValue(sb, "readRequestsCount",
          Long.valueOf(this.readRequestsCount));
      sb = Strings.appendKeyValue(sb, "writeRequestsCount",
          Long.valueOf(this.writeRequestsCount));
      return sb.toString();
    }
  }

  /*
   * TODO: Other metrics that might be considered when the master is actually
   * doing load balancing instead of merely trying to decide where to assign
   * a region:
   * <ul>
   *   <li># of CPUs, heap size (to determine the "class" of machine). For
   *       now, we consider them to be homogeneous.</li>
   *   <li>#requests per region (Map<{String|HRegionInfo}, Integer>)</li>
   *   <li>#compactions and/or #splits (churn)</li>
   *   <li>server death rate (maybe there is something wrong with this server)</li>
   * </ul>
   */

  /** default constructor (used by Writable) */
  public HServerLoad() {
    super();
  }

  /**
   * Constructor
   * @param numberOfRequests
   * @param usedHeapMB
   * @param maxHeapMB
   */
  public HServerLoad(final int numberOfRequests, final int usedHeapMB,
      final int maxHeapMB, final Map<byte[], RegionLoad> regionLoad) {
    this.numberOfRequests = numberOfRequests;
    this.usedHeapMB = usedHeapMB;
    this.maxHeapMB = maxHeapMB;
    this.regionLoad = regionLoad;
  }

  /**
   * Constructor
   * @param hsl the template HServerLoad
   */
  public HServerLoad(final HServerLoad hsl) {
    this(hsl.numberOfRequests, hsl.usedHeapMB, hsl.maxHeapMB, hsl.getRegionsLoad());
    for (Map.Entry<byte[], RegionLoad> e : hsl.regionLoad.entrySet()) {
      this.regionLoad.put(e.getKey(), e.getValue());
    }
  }

  /**
   * Originally, this method factored in the effect of requests going to the
   * server as well. However, this does not interact very well with the current
   * region rebalancing code, which only factors number of regions. For the
   * interim, until we can figure out how to make rebalancing use all the info
   * available, we're just going to make load purely the number of regions.
   *
   * @return load factor for this server
   */
  public int getLoad() {
    // int load = numberOfRequests == 0 ? 1 : numberOfRequests;
    // load *= numberOfRegions == 0 ? 1 : numberOfRegions;
    // return load;
    return this.regionLoad.size();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return toString(1);
  }

  /**
   * Returns toString() with the number of requests divided by the message
   * interval in seconds
   * @param msgInterval
   * @return The load as a String
   */
  public String toString(int msgInterval) {
    int numberOfRegions = this.regionLoad.size();
    StringBuilder sb = new StringBuilder();
    sb = Strings.appendKeyValue(sb, "requests",
      Integer.valueOf(numberOfRequests/msgInterval));
    sb = Strings.appendKeyValue(sb, "regions",
      Integer.valueOf(numberOfRegions));
    sb = Strings.appendKeyValue(sb, "usedHeap",
      Integer.valueOf(this.usedHeapMB));
    sb = Strings.appendKeyValue(sb, "maxHeap", Integer.valueOf(maxHeapMB));
    return sb.toString();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    return compareTo((HServerLoad)o) == 0;
  }

  // Getters

  /**
   * @return the numberOfRegions
   */
  public int getNumberOfRegions() {
    return this.regionLoad.size();
  }

  /**
   * @return the numberOfRequests
   */
  public int getNumberOfRequests() {
    return numberOfRequests;
  }

  /**
   * @return the amount of heap in use, in MB
   */
  public int getUsedHeapMB() {
    return usedHeapMB;
  }

  /**
   * @return the maximum allowable heap size, in MB
   */
  public int getMaxHeapMB() {
    return maxHeapMB;
  }

  /**
   * @return region load metrics
   */
  public Map<byte[], RegionLoad> getRegionsLoad() {
    return Collections.unmodifiableMap(regionLoad);
  }

  /**
   * @return Count of storefiles on this regionserver
   */
  public int getStorefiles() {
    int count = 0;
    for (RegionLoad info: regionLoad.values())
    	count += info.getStorefiles();
    return count;
  }

  /**
   * @return Total size of store files in MB
   */
  public int getStorefileSizeInMB() {
    int count = 0;
    for (RegionLoad info: regionLoad.values())
      count += info.getStorefileSizeMB();
    return count;
  }

  /**
   * @return Size of memstores in MB
   */
  public int getMemStoreSizeInMB() {
    int count = 0;
    for (RegionLoad info: regionLoad.values())
    	count += info.getMemStoreSizeMB();
    return count;
  }

  /**
   * @return Size of store file indexes in MB
   */
  public int getStorefileIndexSizeInMB() {
    int count = 0;
    for (RegionLoad info: regionLoad.values())
    	count += info.getStorefileIndexSizeMB();
    return count;
  }

  // Writable

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int version = in.readByte();
    if (version > VERSION) throw new IOException("Version mismatch; " + version);
    numberOfRequests = in.readInt();
    usedHeapMB = in.readInt();
    maxHeapMB = in.readInt();
    int numberOfRegions = in.readInt();
    for (int i = 0; i < numberOfRegions; i++) {
      RegionLoad rl = new RegionLoad();
      rl.readFields(in);
      regionLoad.put(rl.getName(), rl);
    }
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeByte(VERSION);
    out.writeInt(numberOfRequests);
    out.writeInt(usedHeapMB);
    out.writeInt(maxHeapMB);
    out.writeInt(this.regionLoad.size());
    for (RegionLoad rl: regionLoad.values())
      rl.write(out);
  }

  // Comparable

  public int compareTo(HServerLoad o) {
    return this.getLoad() - o.getLoad();
  }
}
