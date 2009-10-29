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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * An HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 * 
 * It is used as input when creating a table or adding a column. Once set, the
 * parameters that specify a column cannot be changed without deleting the
 * column and recreating it. If there is data stored in the column, it will be
 * deleted when the column is deleted.
 */
public class HColumnDescriptor implements WritableComparable<HColumnDescriptor> {
  // For future backward compatibility

  // Version 3 was when column names become byte arrays and when we picked up
  // Time-to-live feature.  Version 4 was when we moved to byte arrays, HBASE-82.
  // Version 5 was when bloom filter descriptors were removed.
  // Version 6 adds metadata as a map where keys and values are byte[].
  // Version 7 -- add new compression and hfile blocksize to HColumnDescriptor (HBASE-1217)
  private static final byte COLUMN_DESCRIPTOR_VERSION = (byte)7;

  /** 
   * The type of compression.
   * @see org.apache.hadoop.io.SequenceFile.Writer
   * @deprecated Compression now means which compression library
   * rather than 'what' to compress.
   */
  @Deprecated
  public static enum CompressionType {
    /** Do not compress records. */
    NONE, 
    /** Compress values only, each separately. */
    RECORD,
    /** Compress sequences of records together in blocks. */
    BLOCK
  }

  public static final String COMPRESSION = "COMPRESSION";
  public static final String BLOCKCACHE = "BLOCKCACHE";
  public static final String BLOCKSIZE = "BLOCKSIZE";
  public static final String LENGTH = "LENGTH";
  public static final String TTL = "TTL";
  public static final String BLOOMFILTER = "BLOOMFILTER";
  public static final String FOREVER = "FOREVER";
  public static final String MAPFILE_INDEX_INTERVAL =
      "MAPFILE_INDEX_INTERVAL";

  /**
   * Default compression type.
   */
  public static final String DEFAULT_COMPRESSION =
    Compression.Algorithm.NONE.getName();

  /**
   * Default number of versions of a record to keep.
   */
  public static final int DEFAULT_VERSIONS = 3;

  /*
   * Cache here the HCD value.
   * Question: its OK to cache since when we're reenable, we create a new HCD?
   */
  private volatile Integer blocksize = null;

  /**
   * Default setting for whether to serve from memory or not.
   */
  public static final boolean DEFAULT_IN_MEMORY = false;

  /**
   * Default setting for whether to use a block cache or not.
   */
  public static final boolean DEFAULT_BLOCKCACHE = true;

  /**
   * Default size of blocks in files store to the filesytem.  Use smaller for
   * faster random-access at expense of larger indices (more memory consumption).
   */
  public static final int DEFAULT_BLOCKSIZE = HFile.DEFAULT_BLOCKSIZE;

  /**
   * Default setting for whether or not to use bloomfilters.
   */
  public static final boolean DEFAULT_BLOOMFILTER = false;
  
  /**
   * Default time to live of cell contents.
   */
  public static final int DEFAULT_TTL = HConstants.FOREVER;

  // Column family name
  private byte [] name;

  // Column metadata
  protected Map<ImmutableBytesWritable,ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable,ImmutableBytesWritable>();

  /*
   * Cache the max versions rather than calculate it every time.
   */
  private int cachedMaxVersions = -1;

  /**
   * Default constructor. Must be present for Writable.
   */
  public HColumnDescriptor() {
    this.name = null;
  }

  /**
   * Construct a column descriptor specifying only the family name 
   * The other attributes are defaulted.
   * 
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   */
  public HColumnDescriptor(final String familyName) {
    this(Bytes.toBytes(familyName));
  }
  
  /**
   * Construct a column descriptor specifying only the family name 
   * The other attributes are defaulted.
   * 
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   */
  public HColumnDescriptor(final byte [] familyName) {
    this (familyName == null || familyName.length <= 0?
      HConstants.EMPTY_BYTE_ARRAY: familyName, DEFAULT_VERSIONS,
      DEFAULT_COMPRESSION, DEFAULT_IN_MEMORY, DEFAULT_BLOCKCACHE,
      DEFAULT_TTL, false);
  }

  /**
   * Constructor.
   * Makes a deep copy of the supplied descriptor. 
   * Can make a modifiable descriptor from an UnmodifyableHColumnDescriptor.
   * @param desc The descriptor.
   */
  public HColumnDescriptor(HColumnDescriptor desc) {
    super();
    this.name = desc.name.clone();
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        desc.values.entrySet()) {
      this.values.put(e.getKey(), e.getValue());
    }
  }

  /**
   * Constructor
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   * @param maxVersions Maximum number of versions to keep
   * @param compression Compression type
   * @param inMemory If true, column data should be kept in an HRegionServer's
   * cache
   * @param blockCacheEnabled If true, MapFile blocks should be cached
   * @param timeToLive Time-to-live of cell contents, in seconds
   * (use HConstants.FOREVER for unlimited TTL)
   * @param bloomFilter Enable the specified bloom filter for this column
   * 
   * @throws IllegalArgumentException if passed a family name that is made of 
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> or contains
   * a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   */
  public HColumnDescriptor(final byte [] familyName, final int maxVersions,
      final String compression, final boolean inMemory,
      final boolean blockCacheEnabled,
      final int timeToLive, final boolean bloomFilter) {
    this(familyName, maxVersions, compression, inMemory, blockCacheEnabled,
      DEFAULT_BLOCKSIZE, timeToLive, bloomFilter);
  }
  
  /**
   * Constructor
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   * @param maxVersions Maximum number of versions to keep
   * @param compression Compression type
   * @param inMemory If true, column data should be kept in an HRegionServer's
   * cache
   * @param blockCacheEnabled If true, MapFile blocks should be cached
   * @param blocksize
   * @param timeToLive Time-to-live of cell contents, in seconds
   * (use HConstants.FOREVER for unlimited TTL)
   * @param bloomFilter Enable the specified bloom filter for this column
   * 
   * @throws IllegalArgumentException if passed a family name that is made of 
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> or contains
   * a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   */
  public HColumnDescriptor(final byte [] familyName, final int maxVersions,
      final String compression, final boolean inMemory,
      final boolean blockCacheEnabled, final int blocksize,
      final int timeToLive, final boolean bloomFilter) {
    isLegalFamilyName(familyName);
    this.name = familyName;

    if (maxVersions <= 0) {
      // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
      // Until there is support, consider 0 or < 0 -- a configuration error.
      throw new IllegalArgumentException("Maximum versions must be positive");
    }
    setMaxVersions(maxVersions);
    setInMemory(inMemory);
    setBlockCacheEnabled(blockCacheEnabled);
    setTimeToLive(timeToLive);
    setCompressionType(Compression.Algorithm.
      valueOf(compression.toUpperCase()));
    setBloomfilter(bloomFilter);
    setBlocksize(blocksize);
  }

  /**
   * @param b Family name.
   * @return <code>b</code>
   * @throws IllegalArgumentException If not null and not a legitimate family
   * name: i.e. 'printable' and ends in a ':' (Null passes are allowed because
   * <code>b</code> can be null when deserializing).  Cannot start with a '.'
   * either.
   */
  public static byte [] isLegalFamilyName(final byte [] b) {
    if (b == null) {
      return b;
    }
    if (b[0] == '.') {
      throw new IllegalArgumentException("Family names cannot start with a " +
        "period: " + Bytes.toString(b));
    }
    for (int i = 0; i < b.length; i++) {
      if (Character.isISOControl(b[i]) || b[i] == ':') {
        throw new IllegalArgumentException("Illegal character <" + b[i] +
          ">. Family names cannot contain control characters or colons: " +
          Bytes.toString(b));
      }
    }
    return b;
  }

  /**
   * @return Name of this column family
   */
  public byte [] getName() {
    return name;
  }
  
  /**
   * @return Name of this column family
   */
  public String getNameAsString() {
    return Bytes.toString(this.name);
  }

  /**
   * @param key The key.
   * @return The value.
   */
  public byte[] getValue(byte[] key) {
    ImmutableBytesWritable ibw = values.get(new ImmutableBytesWritable(key));
    if (ibw == null)
      return null;
    return ibw.get();
  }

  /**
   * @param key The key.
   * @return The value as a string.
   */
  public String getValue(String key) {
    byte[] value = getValue(Bytes.toBytes(key));
    if (value == null)
      return null;
    return Bytes.toString(value);
  }

  /**
   * @return All values.
   */
  public Map<ImmutableBytesWritable,ImmutableBytesWritable> getValues() {
    return Collections.unmodifiableMap(values);
  }

  /**
   * @param key The key.
   * @param value The value.
   */
  public void setValue(byte[] key, byte[] value) {
    values.put(new ImmutableBytesWritable(key),
      new ImmutableBytesWritable(value));
  }

  /**
   * @param key Key whose key and value we're to remove from HCD parameters.
   */
  public void remove(final byte [] key) {
    values.remove(new ImmutableBytesWritable(key));
  }

  /**
   * @param key The key.
   * @param value The value.
   */
  public void setValue(String key, String value) {
    setValue(Bytes.toBytes(key), Bytes.toBytes(value));
  }

  /** @return compression type being used for the column family */
  public Compression.Algorithm getCompression() {
    String n = getValue(COMPRESSION);
    return Compression.Algorithm.valueOf(n.toUpperCase());
  }
  
  /** @return maximum number of versions */
  public synchronized int getMaxVersions() {
    if (this.cachedMaxVersions == -1) {
      String value = getValue(HConstants.VERSIONS);
      this.cachedMaxVersions = (value != null)?
        Integer.valueOf(value).intValue(): DEFAULT_VERSIONS;
    }
    return this.cachedMaxVersions;
  }

  /**
   * @param maxVersions maximum number of versions
   */
  public void setMaxVersions(int maxVersions) {
    setValue(HConstants.VERSIONS, Integer.toString(maxVersions));
  }

  /**
   * @return Blocksize.
   */
  public synchronized int getBlocksize() {
    if (this.blocksize == null) {
      String value = getValue(BLOCKSIZE);
      this.blocksize = (value != null)?
        Integer.decode(value): Integer.valueOf(DEFAULT_BLOCKSIZE);
    }
    return this.blocksize.intValue();
  }

  /**
   * @param s
   */
  public void setBlocksize(int s) {
    setValue(BLOCKSIZE, Integer.toString(s));
    this.blocksize = null;
  }

  /**
   * @return Compression type setting.
   */
  public Compression.Algorithm getCompressionType() {
    return getCompression();
  }

  /**
   * Compression types supported in hbase.
   * LZO is not bundled as part of the hbase distribution.
   * See <a href="http://wiki.apache.org/hadoop/UsingLzoCompression">LZO Compression</a>
   * for how to enable it.
   * @param type Compression type setting.
   */
  public void setCompressionType(Compression.Algorithm type) {
    String compressionType;
    switch (type) {
      case LZO: compressionType = "LZO"; break;
      case GZ: compressionType = "GZ"; break;
      default: compressionType = "NONE"; break;
    }
    setValue(COMPRESSION, compressionType);
  }

  /**
   * @return True if we are to keep all in use HRegionServer cache.
   */
  public boolean isInMemory() {
    String value = getValue(HConstants.IN_MEMORY);
    if (value != null)
      return Boolean.valueOf(value).booleanValue();
    return DEFAULT_IN_MEMORY;
  }
  
  /**
   * @param inMemory True if we are to keep all values in the HRegionServer
   * cache
   */
  public void setInMemory(boolean inMemory) {
    setValue(HConstants.IN_MEMORY, Boolean.toString(inMemory));
  }

  /**
   * @return Time-to-live of cell contents, in seconds.
   */
  public int getTimeToLive() {
    String value = getValue(TTL);
    return (value != null)? Integer.valueOf(value).intValue(): DEFAULT_TTL;
  }

  /**
   * @param timeToLive Time-to-live of cell contents, in seconds.
   */
  public void setTimeToLive(int timeToLive) {
    setValue(TTL, Integer.toString(timeToLive));
  }

  /**
   * @return True if MapFile blocks should be cached.
   */
  public boolean isBlockCacheEnabled() {
    String value = getValue(BLOCKCACHE);
    if (value != null)
      return Boolean.valueOf(value).booleanValue();
    return DEFAULT_BLOCKCACHE;
  }

  /**
   * @param blockCacheEnabled True if MapFile blocks should be cached.
   */
  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    setValue(BLOCKCACHE, Boolean.toString(blockCacheEnabled));
  }

  /**
   * @return true if a bloom filter is enabled
   */
  public boolean isBloomfilter() {
    String value = getValue(BLOOMFILTER);
    if (value != null)
      return Boolean.valueOf(value).booleanValue();
    return DEFAULT_BLOOMFILTER;
  }

  /**
   * @param onOff Enable/Disable bloom filter
   */
  public void setBloomfilter(final boolean onOff) {
    setValue(BLOOMFILTER, Boolean.toString(onOff));
  }

  /**
   * @param interval The number of entries that are added to the store MapFile before
   * an index entry is added.
   */
  public void setMapFileIndexInterval(int interval) {
    setValue(MAPFILE_INDEX_INTERVAL, Integer.toString(interval));
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuffer s = new StringBuffer();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(Bytes.toString(name));
    s.append("'");
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      String key = Bytes.toString(e.getKey().get());
      String value = Bytes.toString(e.getValue().get());
      if (key != null && key.toUpperCase().equals(BLOOMFILTER)) {
        // Don't emit bloomfilter.  Its not working.
        continue;
      }
      s.append(", ");
      s.append(key);
      s.append(" => '");
      s.append(value);
      s.append("'");
    }
    s.append('}');
    return s.toString();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HColumnDescriptor)) {
      return false;
    }
    return compareTo((HColumnDescriptor)obj) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.name);
    result ^= Byte.valueOf(COLUMN_DESCRIPTOR_VERSION).hashCode();
    result ^= values.hashCode();
    return result;
  }
  
  // Writable

  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();
    if (version < 6) {
      if (version <= 2) {
        Text t = new Text();
        t.readFields(in);
        this.name = t.getBytes();
//        if(KeyValue.getFamilyDelimiterIndex(this.name, 0, this.name.length)
//            > 0) {
//          this.name = stripColon(this.name);
//        }
      } else {
        this.name = Bytes.readByteArray(in);
      }
      this.values.clear();
      setMaxVersions(in.readInt());
      int ordinal = in.readInt();
      setCompressionType(Compression.Algorithm.values()[ordinal]);
      setInMemory(in.readBoolean());
      setBloomfilter(in.readBoolean());
      if (isBloomfilter() && version < 5) {
        // If a bloomFilter is enabled and the column descriptor is less than
        // version 5, we need to skip over it to read the rest of the column
        // descriptor. There are no BloomFilterDescriptors written to disk for
        // column descriptors with a version number >= 5
        throw new UnsupportedClassVersionError(this.getClass().getName() +
            " does not support backward compatibility with versions older " +
            "than version 5");
      }
      if (version > 1) {
        setBlockCacheEnabled(in.readBoolean());
      }
      if (version > 2) {
       setTimeToLive(in.readInt());
      }
    } else {
      // version 7+
      this.name = Bytes.readByteArray(in);
      this.values.clear();
      int numValues = in.readInt();
      for (int i = 0; i < numValues; i++) {
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        key.readFields(in);
        value.readFields(in);
        values.put(key, value);
      }
      if (version == 6) {
        // Convert old values.
        setValue(COMPRESSION, Compression.Algorithm.NONE.getName());
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(COLUMN_DESCRIPTOR_VERSION);
    Bytes.writeByteArray(out, this.name);
    out.writeInt(values.size());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
  }

  // Comparable

  public int compareTo(HColumnDescriptor o) {
    int result = Bytes.compareTo(this.name, o.getName());
    if (result == 0) {
      // punt on comparison for ordering, just calculate difference
      result = this.values.hashCode() - o.values.hashCode();
      if (result < 0)
        result = -1;
      else if (result > 0)
        result = 1;
    }
    return result;
  }
}
