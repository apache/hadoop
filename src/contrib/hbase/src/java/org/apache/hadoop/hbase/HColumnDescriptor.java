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

/**
 * A HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 */
public class HColumnDescriptor implements WritableComparable {
  
  // For future backward compatibility
  
  private static final byte COLUMN_DESCRIPTOR_VERSION = (byte)1;
  
  // Legal family names can only contain 'word characters' and end in a colon.

  private static final Pattern LEGAL_FAMILY_NAME = Pattern.compile("\\w+:");

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
  
  // Internal values for compression type used for serialization
  
  private static final byte COMPRESSION_NONE = (byte)0;
  private static final byte COMPRESSION_RECORD = (byte)1;
  private static final byte COMPRESSION_BLOCK = (byte)2;
  
  private static final int DEFAULT_N_VERSIONS = 3;
  
  Text name;                                    // Column family name
  int maxVersions;                              // Number of versions to keep
  byte compressionType;                         // Compression setting if any
  boolean inMemory;                             // Serve reads from in-memory cache
  int maxValueLength;                           // Maximum value size
  private boolean bloomFilterSpecified;         // True if bloom filter was specified
  BloomFilterDescriptor bloomFilter;            // Descriptor of bloom filter
  byte versionNumber;                           // Version number of this class
  
  /**
   * Default constructor. Must be present for Writable.
   */
  public HColumnDescriptor() {
    this.name = new Text();
    this.maxVersions = DEFAULT_N_VERSIONS;
    this.compressionType = COMPRESSION_NONE;
    this.inMemory = false;
    this.maxValueLength = Integer.MAX_VALUE;
    this.bloomFilterSpecified = false;
    this.bloomFilter = null;
    this.versionNumber = COLUMN_DESCRIPTOR_VERSION;
  }
  
  /**
   * Construct a column descriptor specifying only the family name 
   * The other attributes are defaulted.
   * 
   * @param columnName - column family name
   */
  public HColumnDescriptor(String columnName) {
    this();
    this.name.set(columnName);
  }
  
  /**
   * Constructor - specify all parameters.
   * @param name                - Column family name
   * @param maxVersions         - Maximum number of versions to keep
   * @param compression         - Compression type
   * @param inMemory            - If true, column data should be kept in a
   *                              HRegionServer's cache
   * @param maxValueLength      - Restrict values to &lt;= this value
   * @param bloomFilter         - Enable the specified bloom filter for this column
   * 
   * @throws IllegalArgumentException if passed a family name that is made of 
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> and does not
   * end in a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   */
  public HColumnDescriptor(Text name, int maxVersions, CompressionType compression,
      boolean inMemory, int maxValueLength, BloomFilterDescriptor bloomFilter) {
    String familyStr = name.toString();
    Matcher m = LEGAL_FAMILY_NAME.matcher(familyStr);
    if(m == null || !m.matches()) {
      throw new IllegalArgumentException(
          "Family names can only contain 'word characters' and must end with a ':'");
    }
    this.name = name;

    if(maxVersions <= 0) {
      // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
      // Until there is support, consider 0 or < 0 -- a configuration error.
      throw new IllegalArgumentException("Maximum versions must be positive");
    }
    this.maxVersions = maxVersions;

    if(compression == CompressionType.NONE) {
      this.compressionType = COMPRESSION_NONE;

    } else if(compression == CompressionType.BLOCK) {
      this.compressionType = COMPRESSION_BLOCK;
      
    } else if(compression == CompressionType.RECORD) {
      this.compressionType = COMPRESSION_RECORD;
      
    } else {
      assert(false);
    }
    this.inMemory = inMemory;
    this.maxValueLength = maxValueLength;
    this.bloomFilter = bloomFilter;
    this.bloomFilterSpecified = this.bloomFilter == null ? false : true;
    this.versionNumber = COLUMN_DESCRIPTOR_VERSION;
  }
  
  /**
   * @return    - name of column family
   */
  public Text getName() {
    return name;
  }
  
  /**
   * @return    - compression type being used for the column family
   */
  public CompressionType getCompression() {
    CompressionType value = null;

    if(this.compressionType == COMPRESSION_NONE) {
      value = CompressionType.NONE;
      
    } else if(this.compressionType == COMPRESSION_BLOCK) {
      value = CompressionType.BLOCK;
      
    } else if(this.compressionType == COMPRESSION_RECORD) {
      value = CompressionType.RECORD;
      
    } else {
      assert(false);
    }
    return value;
  }
  
  /**
   * @return    - maximum number of versions
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }
  
  @Override
  public String toString() {
    String compression = "none";
    switch(compressionType) {
    case COMPRESSION_NONE:
      break;
    case COMPRESSION_RECORD:
      compression = "record";
      break;
    case COMPRESSION_BLOCK:
      compression = "block";
      break;
    default:
      assert(false);
    }
    
    return "(" + name + ", max versions: " + maxVersions + ", compression: "
      + compression + ", in memory: " + inMemory + ", max value length: "
      + maxValueLength + ", bloom filter: "
      + (bloomFilterSpecified ? bloomFilter.toString() : "none") + ")";
  }
  
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  @Override
  public int hashCode() {
    int result = this.name.hashCode();
    result ^= Integer.valueOf(this.maxVersions).hashCode();
    result ^= Byte.valueOf(this.compressionType).hashCode();
    result ^= Boolean.valueOf(this.inMemory).hashCode();
    result ^= Integer.valueOf(this.maxValueLength).hashCode();
    result ^= Boolean.valueOf(this.bloomFilterSpecified).hashCode();
    result ^= Byte.valueOf(this.versionNumber).hashCode();
    if(this.bloomFilterSpecified) {
      result ^= this.bloomFilter.hashCode();
    }
    return result;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void readFields(DataInput in) throws IOException {
    this.versionNumber = in.readByte();
    this.name.readFields(in);
    this.maxVersions = in.readInt();
    this.compressionType = in.readByte();
    this.inMemory = in.readBoolean();
    this.maxValueLength = in.readInt();
    this.bloomFilterSpecified = in.readBoolean();
    
    if(bloomFilterSpecified) {
      bloomFilter = new BloomFilterDescriptor();
      bloomFilter.readFields(in);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(this.versionNumber);
    this.name.write(out);
    out.writeInt(this.maxVersions);
    out.writeByte(this.compressionType);
    out.writeBoolean(this.inMemory);
    out.writeInt(this.maxValueLength);
    out.writeBoolean(this.bloomFilterSpecified);
    
    if(bloomFilterSpecified) {
      bloomFilter.write(out);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////////////////////////////////////

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
      result = Integer.valueOf(this.compressionType).compareTo(
          Integer.valueOf(other.compressionType));
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
