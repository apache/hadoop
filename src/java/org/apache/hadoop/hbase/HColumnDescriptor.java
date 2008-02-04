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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.hbase.io.TextSequence;

/**
 * An HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 * 
 * It is used as input when creating a table or adding a column. Once set, the
 * parameters that specify a column cannot be changed without deleting the
 * column and recreating it. If there is data stored in the column, it will be
 * deleted when the column is deleted.
 */
public class HColumnDescriptor implements WritableComparable {
  
  // For future backward compatibility
  private static final byte COLUMN_DESCRIPTOR_VERSION = (byte)1;
  
  /** Legal family names can only contain 'word characters' and end in a colon. */
  public static final Pattern LEGAL_FAMILY_NAME = Pattern.compile("\\w+:");

  /** 
   * The type of compression.
   * @see org.apache.hadoop.io.SequenceFile.Writer
   */
  public static enum CompressionType {
    /** Do not compress records. */
    NONE, 
    /** Compress values only, each separately. */
    RECORD,
    /** Compress sequences of records together in blocks. */
    BLOCK
  }
  
  /**
   * Default compression type.
   */
  public static final CompressionType DEFAULT_COMPRESSION_TYPE =
    CompressionType.NONE;
  
  /**
   * Default number of versions of a record to keep.
   */
  public static final int DEFAULT_N_VERSIONS = 3;
  
  /**
   * Default setting for whether to serve from memory or not.
   */
  public static final boolean DEFAULT_IN_MEMORY = false;
  
  /**
   * Default maximum length of cell contents.
   */
  public static final int DEFAULT_MAX_VALUE_LENGTH = Integer.MAX_VALUE;
  
  /**
   * Default bloom filter description.
   */
  public static final BloomFilterDescriptor DEFAULT_BLOOM_FILTER_DESCRIPTOR =
    null;
  
  // Column family name
  private Text name;
  // Number of versions to keep
  private int maxVersions;
  // Compression setting if any
  private CompressionType compressionType;
  // Serve reads from in-memory cache
  private boolean inMemory;
  // Maximum value size
  private int maxValueLength;
  // True if bloom filter was specified
  private boolean bloomFilterSpecified;
  // Descriptor of bloom filter
  private BloomFilterDescriptor bloomFilter;
  // Version number of this class
  private byte versionNumber;
  // Family name without the ':'
  private transient Text familyName = null;
  
  /**
   * Default constructor. Must be present for Writable.
   */
  public HColumnDescriptor() {
    this(null);
  }
  
  /**
   * Construct a column descriptor specifying only the family name 
   * The other attributes are defaulted.
   * 
   * @param columnName - column family name
   */
  public HColumnDescriptor(String columnName) {
    this(columnName == null || columnName.length() <= 0?
      new Text(): new Text(columnName),
      DEFAULT_N_VERSIONS, DEFAULT_COMPRESSION_TYPE, DEFAULT_IN_MEMORY,
      Integer.MAX_VALUE, DEFAULT_BLOOM_FILTER_DESCRIPTOR);
  }
  
  /**
   * Constructor
   * Specify all parameters.
   * @param name Column family name
   * @param maxVersions Maximum number of versions to keep
   * @param compression Compression type
   * @param inMemory If true, column data should be kept in an HRegionServer's
   * cache
   * @param maxValueLength Restrict values to &lt;= this value
   * @param bloomFilter Enable the specified bloom filter for this column
   * 
   * @throws IllegalArgumentException if passed a family name that is made of 
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> and does not
   * end in a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   */
  public HColumnDescriptor(final Text name, final int maxVersions,
      final CompressionType compression, final boolean inMemory,
      final int maxValueLength, final BloomFilterDescriptor bloomFilter) {
    String familyStr = name.toString();
    // Test name if not null (It can be null when deserializing after
    // construction but before we've read in the fields);
    if (familyStr.length() > 0) {
      Matcher m = LEGAL_FAMILY_NAME.matcher(familyStr);
      if(m == null || !m.matches()) {
        throw new IllegalArgumentException("Illegal family name <" + name +
          ">. Family names can only contain " +
          "'word characters' and must end with a ':'");
      }
    }
    this.name = name;

    if(maxVersions <= 0) {
      // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
      // Until there is support, consider 0 or < 0 -- a configuration error.
      throw new IllegalArgumentException("Maximum versions must be positive");
    }
    this.maxVersions = maxVersions;
    this.inMemory = inMemory;
    this.maxValueLength = maxValueLength;
    this.bloomFilter = bloomFilter;
    this.bloomFilterSpecified = this.bloomFilter == null ? false : true;
    this.versionNumber = COLUMN_DESCRIPTOR_VERSION;
    this.compressionType = compression;
  }
  
  /** @return name of column family */
  public Text getName() {
    return name;
  }

  /** @return name of column family without trailing ':' */
  public synchronized Text getFamilyName() {
    if (name != null) {
      if (familyName == null) {
        familyName = new TextSequence(name, 0, name.getLength() - 1).toText();
      }
      return familyName;
    }
    return null;
  }
  
  /** @return compression type being used for the column family */
  public CompressionType getCompression() {
    return this.compressionType;
  }
  
  /** @return maximum number of versions */
  public int getMaxVersions() {
    return this.maxVersions;
  }
  
  /**
   * @return Compression type setting.
   */
  public CompressionType getCompressionType() {
    return this.compressionType;
  }

  /**
   * @return True if we are to keep all in use HRegionServer cache.
   */
  public boolean isInMemory() {
    return this.inMemory;
  }
  
  /**
   * @return Maximum value length.
   */
  public int getMaxValueLength() {
    return this.maxValueLength;
  }

  /**
   * @return Bloom filter descriptor or null if none set.
   */
  public BloomFilterDescriptor getBloomFilter() {
    return this.bloomFilter;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    // Output a name minus ':'.
    String tmp = name.toString();
    return "{name: " + tmp.substring(0, tmp.length() - 1) +
      ", max versions: " + maxVersions +
      ", compression: " + this.compressionType + ", in memory: " + inMemory +
      ", max length: " + maxValueLength + ", bloom filter: " +
      (bloomFilterSpecified ? bloomFilter.toString() : "none") + "}";
  }
  
  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = this.name.hashCode();
    result ^= Integer.valueOf(this.maxVersions).hashCode();
    result ^= this.compressionType.hashCode();
    result ^= Boolean.valueOf(this.inMemory).hashCode();
    result ^= Integer.valueOf(this.maxValueLength).hashCode();
    result ^= Boolean.valueOf(this.bloomFilterSpecified).hashCode();
    result ^= Byte.valueOf(this.versionNumber).hashCode();
    if(this.bloomFilterSpecified) {
      result ^= this.bloomFilter.hashCode();
    }
    return result;
  }
  
  // Writable

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.versionNumber = in.readByte();
    this.name.readFields(in);
    this.maxVersions = in.readInt();
    int ordinal = in.readInt();
    this.compressionType = CompressionType.values()[ordinal];
    this.inMemory = in.readBoolean();
    this.maxValueLength = in.readInt();
    this.bloomFilterSpecified = in.readBoolean();
    
    if(bloomFilterSpecified) {
      bloomFilter = new BloomFilterDescriptor();
      bloomFilter.readFields(in);
    }
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeByte(this.versionNumber);
    this.name.write(out);
    out.writeInt(this.maxVersions);
    out.writeInt(this.compressionType.ordinal());
    out.writeBoolean(this.inMemory);
    out.writeInt(this.maxValueLength);
    out.writeBoolean(this.bloomFilterSpecified);
    
    if(bloomFilterSpecified) {
      bloomFilter.write(out);
    }
  }

  // Comparable

  /** {@inheritDoc} */
  public int compareTo(Object o) {
    // NOTE: we don't do anything with the version number yet.
    // Version numbers will come into play when we introduce an incompatible
    // change in the future such as the addition of access control lists.
    
    HColumnDescriptor other = (HColumnDescriptor)o;
    
    int result = this.name.compareTo(other.getName());
    
    if(result == 0) {
      result = Integer.valueOf(this.maxVersions).compareTo(
          Integer.valueOf(other.maxVersions));
    }
    
    if(result == 0) {
      result = this.compressionType.compareTo(other.compressionType);
    }
    
    if(result == 0) {
      if(this.inMemory == other.inMemory) {
        result = 0;
        
      } else if(this.inMemory) {
        result = -1;
        
      } else {
        result = 1;
      }
    }
    
    if(result == 0) {
      result = other.maxValueLength - this.maxValueLength;
    }
    
    if(result == 0) {
      if(this.bloomFilterSpecified == other.bloomFilterSpecified) {
        result = 0;
        
      } else if(this.bloomFilterSpecified) {
        result = -1;
        
      } else {
        result = 1;
      }
    }
    
    if(result == 0 && this.bloomFilterSpecified) {
      result = this.bloomFilter.compareTo(other.bloomFilter);
    }
    
    return result;
  }
}
