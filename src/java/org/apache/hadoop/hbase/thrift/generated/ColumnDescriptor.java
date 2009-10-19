/*
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
package org.apache.hadoop.hbase.thrift.generated;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

/**
 * An HColumnDescriptor contains information about a column family
 * such as the number of versions, compression settings, etc. It is
 * used as input when creating a table or adding a column.
 */
public class ColumnDescriptor implements TBase, java.io.Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  private static final TStruct STRUCT_DESC = new TStruct("ColumnDescriptor");
  private static final TField NAME_FIELD_DESC = new TField("name", TType.STRING, (short)1);
  private static final TField MAX_VERSIONS_FIELD_DESC = new TField("maxVersions", TType.I32, (short)2);
  private static final TField COMPRESSION_FIELD_DESC = new TField("compression", TType.STRING, (short)3);
  private static final TField IN_MEMORY_FIELD_DESC = new TField("inMemory", TType.BOOL, (short)4);
  private static final TField BLOOM_FILTER_TYPE_FIELD_DESC = new TField("bloomFilterType", TType.STRING, (short)5);
  private static final TField BLOOM_FILTER_VECTOR_SIZE_FIELD_DESC = new TField("bloomFilterVectorSize", TType.I32, (short)6);
  private static final TField BLOOM_FILTER_NB_HASHES_FIELD_DESC = new TField("bloomFilterNbHashes", TType.I32, (short)7);
  private static final TField BLOCK_CACHE_ENABLED_FIELD_DESC = new TField("blockCacheEnabled", TType.BOOL, (short)8);
  private static final TField TIME_TO_LIVE_FIELD_DESC = new TField("timeToLive", TType.I32, (short)9);

  public byte[] name;
  public static final int NAME = 1;
  public int maxVersions;
  public static final int MAXVERSIONS = 2;
  public String compression;
  public static final int COMPRESSION = 3;
  public boolean inMemory;
  public static final int INMEMORY = 4;
  public String bloomFilterType;
  public static final int BLOOMFILTERTYPE = 5;
  public int bloomFilterVectorSize;
  public static final int BLOOMFILTERVECTORSIZE = 6;
  public int bloomFilterNbHashes;
  public static final int BLOOMFILTERNBHASHES = 7;
  public boolean blockCacheEnabled;
  public static final int BLOCKCACHEENABLED = 8;
  public int timeToLive;
  public static final int TIMETOLIVE = 9;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    public boolean maxVersions = false;
    public boolean inMemory = false;
    public boolean bloomFilterVectorSize = false;
    public boolean bloomFilterNbHashes = false;
    public boolean blockCacheEnabled = false;
    public boolean timeToLive = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(NAME, new FieldMetaData("name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(MAXVERSIONS, new FieldMetaData("maxVersions", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(COMPRESSION, new FieldMetaData("compression", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(INMEMORY, new FieldMetaData("inMemory", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.BOOL)));
    put(BLOOMFILTERTYPE, new FieldMetaData("bloomFilterType", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(BLOOMFILTERVECTORSIZE, new FieldMetaData("bloomFilterVectorSize", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(BLOOMFILTERNBHASHES, new FieldMetaData("bloomFilterNbHashes", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(BLOCKCACHEENABLED, new FieldMetaData("blockCacheEnabled", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.BOOL)));
    put(TIMETOLIVE, new FieldMetaData("timeToLive", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(ColumnDescriptor.class, metaDataMap);
  }

  public ColumnDescriptor() {
    this.maxVersions = 3;

    this.compression = "NONE";

    this.inMemory = false;

    this.bloomFilterType = "NONE";

    this.bloomFilterVectorSize = 0;

    this.bloomFilterNbHashes = 0;

    this.blockCacheEnabled = false;

    this.timeToLive = -1;

  }

  public ColumnDescriptor(
    byte[] name,
    int maxVersions,
    String compression,
    boolean inMemory,
    String bloomFilterType,
    int bloomFilterVectorSize,
    int bloomFilterNbHashes,
    boolean blockCacheEnabled,
    int timeToLive)
  {
    this();
    this.name = name;
    this.maxVersions = maxVersions;
    this.__isset.maxVersions = true;
    this.compression = compression;
    this.inMemory = inMemory;
    this.__isset.inMemory = true;
    this.bloomFilterType = bloomFilterType;
    this.bloomFilterVectorSize = bloomFilterVectorSize;
    this.__isset.bloomFilterVectorSize = true;
    this.bloomFilterNbHashes = bloomFilterNbHashes;
    this.__isset.bloomFilterNbHashes = true;
    this.blockCacheEnabled = blockCacheEnabled;
    this.__isset.blockCacheEnabled = true;
    this.timeToLive = timeToLive;
    this.__isset.timeToLive = true;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ColumnDescriptor(ColumnDescriptor other) {
    if (other.isSetName()) {
      this.name = other.name;
    }
    __isset.maxVersions = other.__isset.maxVersions;
    this.maxVersions = other.maxVersions;
    if (other.isSetCompression()) {
      this.compression = other.compression;
    }
    __isset.inMemory = other.__isset.inMemory;
    this.inMemory = other.inMemory;
    if (other.isSetBloomFilterType()) {
      this.bloomFilterType = other.bloomFilterType;
    }
    __isset.bloomFilterVectorSize = other.__isset.bloomFilterVectorSize;
    this.bloomFilterVectorSize = other.bloomFilterVectorSize;
    __isset.bloomFilterNbHashes = other.__isset.bloomFilterNbHashes;
    this.bloomFilterNbHashes = other.bloomFilterNbHashes;
    __isset.blockCacheEnabled = other.__isset.blockCacheEnabled;
    this.blockCacheEnabled = other.blockCacheEnabled;
    __isset.timeToLive = other.__isset.timeToLive;
    this.timeToLive = other.timeToLive;
  }

  @Override
  public ColumnDescriptor clone() {
    return new ColumnDescriptor(this);
  }

  public byte[] getName() {
    return this.name;
  }

  public void setName(byte[] name) {
    this.name = name;
  }

  public void unsetName() {
    this.name = null;
  }

  // Returns true if field name is set (has been asigned a value) and false otherwise
  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  public int getMaxVersions() {
    return this.maxVersions;
  }

  public void setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
    this.__isset.maxVersions = true;
  }

  public void unsetMaxVersions() {
    this.__isset.maxVersions = false;
  }

  // Returns true if field maxVersions is set (has been asigned a value) and false otherwise
  public boolean isSetMaxVersions() {
    return this.__isset.maxVersions;
  }

  public void setMaxVersionsIsSet(boolean value) {
    this.__isset.maxVersions = value;
  }

  public String getCompression() {
    return this.compression;
  }

  public void setCompression(String compression) {
    this.compression = compression;
  }

  public void unsetCompression() {
    this.compression = null;
  }

  // Returns true if field compression is set (has been asigned a value) and false otherwise
  public boolean isSetCompression() {
    return this.compression != null;
  }

  public void setCompressionIsSet(boolean value) {
    if (!value) {
      this.compression = null;
    }
  }

  public boolean isInMemory() {
    return this.inMemory;
  }

  public void setInMemory(boolean inMemory) {
    this.inMemory = inMemory;
    this.__isset.inMemory = true;
  }

  public void unsetInMemory() {
    this.__isset.inMemory = false;
  }

  // Returns true if field inMemory is set (has been asigned a value) and false otherwise
  public boolean isSetInMemory() {
    return this.__isset.inMemory;
  }

  public void setInMemoryIsSet(boolean value) {
    this.__isset.inMemory = value;
  }

  public String getBloomFilterType() {
    return this.bloomFilterType;
  }

  public void setBloomFilterType(String bloomFilterType) {
    this.bloomFilterType = bloomFilterType;
  }

  public void unsetBloomFilterType() {
    this.bloomFilterType = null;
  }

  // Returns true if field bloomFilterType is set (has been asigned a value) and false otherwise
  public boolean isSetBloomFilterType() {
    return this.bloomFilterType != null;
  }

  public void setBloomFilterTypeIsSet(boolean value) {
    if (!value) {
      this.bloomFilterType = null;
    }
  }

  public int getBloomFilterVectorSize() {
    return this.bloomFilterVectorSize;
  }

  public void setBloomFilterVectorSize(int bloomFilterVectorSize) {
    this.bloomFilterVectorSize = bloomFilterVectorSize;
    this.__isset.bloomFilterVectorSize = true;
  }

  public void unsetBloomFilterVectorSize() {
    this.__isset.bloomFilterVectorSize = false;
  }

  // Returns true if field bloomFilterVectorSize is set (has been asigned a value) and false otherwise
  public boolean isSetBloomFilterVectorSize() {
    return this.__isset.bloomFilterVectorSize;
  }

  public void setBloomFilterVectorSizeIsSet(boolean value) {
    this.__isset.bloomFilterVectorSize = value;
  }

  public int getBloomFilterNbHashes() {
    return this.bloomFilterNbHashes;
  }

  public void setBloomFilterNbHashes(int bloomFilterNbHashes) {
    this.bloomFilterNbHashes = bloomFilterNbHashes;
    this.__isset.bloomFilterNbHashes = true;
  }

  public void unsetBloomFilterNbHashes() {
    this.__isset.bloomFilterNbHashes = false;
  }

  // Returns true if field bloomFilterNbHashes is set (has been asigned a value) and false otherwise
  public boolean isSetBloomFilterNbHashes() {
    return this.__isset.bloomFilterNbHashes;
  }

  public void setBloomFilterNbHashesIsSet(boolean value) {
    this.__isset.bloomFilterNbHashes = value;
  }

  public boolean isBlockCacheEnabled() {
    return this.blockCacheEnabled;
  }

  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    this.blockCacheEnabled = blockCacheEnabled;
    this.__isset.blockCacheEnabled = true;
  }

  public void unsetBlockCacheEnabled() {
    this.__isset.blockCacheEnabled = false;
  }

  // Returns true if field blockCacheEnabled is set (has been asigned a value) and false otherwise
  public boolean isSetBlockCacheEnabled() {
    return this.__isset.blockCacheEnabled;
  }

  public void setBlockCacheEnabledIsSet(boolean value) {
    this.__isset.blockCacheEnabled = value;
  }

  public int getTimeToLive() {
    return this.timeToLive;
  }

  public void setTimeToLive(int timeToLive) {
    this.timeToLive = timeToLive;
    this.__isset.timeToLive = true;
  }

  public void unsetTimeToLive() {
    this.__isset.timeToLive = false;
  }

  // Returns true if field timeToLive is set (has been asigned a value) and false otherwise
  public boolean isSetTimeToLive() {
    return this.__isset.timeToLive;
  }

  public void setTimeToLiveIsSet(boolean value) {
    this.__isset.timeToLive = value;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((byte[])value);
      }
      break;

    case MAXVERSIONS:
      if (value == null) {
        unsetMaxVersions();
      } else {
        setMaxVersions((Integer)value);
      }
      break;

    case COMPRESSION:
      if (value == null) {
        unsetCompression();
      } else {
        setCompression((String)value);
      }
      break;

    case INMEMORY:
      if (value == null) {
        unsetInMemory();
      } else {
        setInMemory((Boolean)value);
      }
      break;

    case BLOOMFILTERTYPE:
      if (value == null) {
        unsetBloomFilterType();
      } else {
        setBloomFilterType((String)value);
      }
      break;

    case BLOOMFILTERVECTORSIZE:
      if (value == null) {
        unsetBloomFilterVectorSize();
      } else {
        setBloomFilterVectorSize((Integer)value);
      }
      break;

    case BLOOMFILTERNBHASHES:
      if (value == null) {
        unsetBloomFilterNbHashes();
      } else {
        setBloomFilterNbHashes((Integer)value);
      }
      break;

    case BLOCKCACHEENABLED:
      if (value == null) {
        unsetBlockCacheEnabled();
      } else {
        setBlockCacheEnabled((Boolean)value);
      }
      break;

    case TIMETOLIVE:
      if (value == null) {
        unsetTimeToLive();
      } else {
        setTimeToLive((Integer)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case NAME:
      return getName();

    case MAXVERSIONS:
      return Integer.valueOf(getMaxVersions());

    case COMPRESSION:
      return getCompression();

    case INMEMORY:
      return Boolean.valueOf(isInMemory());

    case BLOOMFILTERTYPE:
      return getBloomFilterType();

    case BLOOMFILTERVECTORSIZE:
      return Integer.valueOf(getBloomFilterVectorSize());

    case BLOOMFILTERNBHASHES:
      return Integer.valueOf(getBloomFilterNbHashes());

    case BLOCKCACHEENABLED:
      return Boolean.valueOf(isBlockCacheEnabled());

    case TIMETOLIVE:
      return Integer.valueOf(getTimeToLive());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case NAME:
      return isSetName();
    case MAXVERSIONS:
      return isSetMaxVersions();
    case COMPRESSION:
      return isSetCompression();
    case INMEMORY:
      return isSetInMemory();
    case BLOOMFILTERTYPE:
      return isSetBloomFilterType();
    case BLOOMFILTERVECTORSIZE:
      return isSetBloomFilterVectorSize();
    case BLOOMFILTERNBHASHES:
      return isSetBloomFilterNbHashes();
    case BLOCKCACHEENABLED:
      return isSetBlockCacheEnabled();
    case TIMETOLIVE:
      return isSetTimeToLive();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ColumnDescriptor)
      return this.equals((ColumnDescriptor)that);
    return false;
  }

  public boolean equals(ColumnDescriptor that) {
    if (that == null)
      return false;

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!java.util.Arrays.equals(this.name, that.name))
        return false;
    }

    boolean this_present_maxVersions = true;
    boolean that_present_maxVersions = true;
    if (this_present_maxVersions || that_present_maxVersions) {
      if (!(this_present_maxVersions && that_present_maxVersions))
        return false;
      if (this.maxVersions != that.maxVersions)
        return false;
    }

    boolean this_present_compression = true && this.isSetCompression();
    boolean that_present_compression = true && that.isSetCompression();
    if (this_present_compression || that_present_compression) {
      if (!(this_present_compression && that_present_compression))
        return false;
      if (!this.compression.equals(that.compression))
        return false;
    }

    boolean this_present_inMemory = true;
    boolean that_present_inMemory = true;
    if (this_present_inMemory || that_present_inMemory) {
      if (!(this_present_inMemory && that_present_inMemory))
        return false;
      if (this.inMemory != that.inMemory)
        return false;
    }

    boolean this_present_bloomFilterType = true && this.isSetBloomFilterType();
    boolean that_present_bloomFilterType = true && that.isSetBloomFilterType();
    if (this_present_bloomFilterType || that_present_bloomFilterType) {
      if (!(this_present_bloomFilterType && that_present_bloomFilterType))
        return false;
      if (!this.bloomFilterType.equals(that.bloomFilterType))
        return false;
    }

    boolean this_present_bloomFilterVectorSize = true;
    boolean that_present_bloomFilterVectorSize = true;
    if (this_present_bloomFilterVectorSize || that_present_bloomFilterVectorSize) {
      if (!(this_present_bloomFilterVectorSize && that_present_bloomFilterVectorSize))
        return false;
      if (this.bloomFilterVectorSize != that.bloomFilterVectorSize)
        return false;
    }

    boolean this_present_bloomFilterNbHashes = true;
    boolean that_present_bloomFilterNbHashes = true;
    if (this_present_bloomFilterNbHashes || that_present_bloomFilterNbHashes) {
      if (!(this_present_bloomFilterNbHashes && that_present_bloomFilterNbHashes))
        return false;
      if (this.bloomFilterNbHashes != that.bloomFilterNbHashes)
        return false;
    }

    boolean this_present_blockCacheEnabled = true;
    boolean that_present_blockCacheEnabled = true;
    if (this_present_blockCacheEnabled || that_present_blockCacheEnabled) {
      if (!(this_present_blockCacheEnabled && that_present_blockCacheEnabled))
        return false;
      if (this.blockCacheEnabled != that.blockCacheEnabled)
        return false;
    }

    boolean this_present_timeToLive = true;
    boolean that_present_timeToLive = true;
    if (this_present_timeToLive || that_present_timeToLive) {
      if (!(this_present_timeToLive && that_present_timeToLive))
        return false;
      if (this.timeToLive != that.timeToLive)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case NAME:
          if (field.type == TType.STRING) {
            this.name = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case MAXVERSIONS:
          if (field.type == TType.I32) {
            this.maxVersions = iprot.readI32();
            this.__isset.maxVersions = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case COMPRESSION:
          if (field.type == TType.STRING) {
            this.compression = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case INMEMORY:
          if (field.type == TType.BOOL) {
            this.inMemory = iprot.readBool();
            this.__isset.inMemory = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case BLOOMFILTERTYPE:
          if (field.type == TType.STRING) {
            this.bloomFilterType = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case BLOOMFILTERVECTORSIZE:
          if (field.type == TType.I32) {
            this.bloomFilterVectorSize = iprot.readI32();
            this.__isset.bloomFilterVectorSize = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case BLOOMFILTERNBHASHES:
          if (field.type == TType.I32) {
            this.bloomFilterNbHashes = iprot.readI32();
            this.__isset.bloomFilterNbHashes = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case BLOCKCACHEENABLED:
          if (field.type == TType.BOOL) {
            this.blockCacheEnabled = iprot.readBool();
            this.__isset.blockCacheEnabled = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case TIMETOLIVE:
          if (field.type == TType.I32) {
            this.timeToLive = iprot.readI32();
            this.__isset.timeToLive = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();


    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.name != null) {
      oprot.writeFieldBegin(NAME_FIELD_DESC);
      oprot.writeBinary(this.name);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(MAX_VERSIONS_FIELD_DESC);
    oprot.writeI32(this.maxVersions);
    oprot.writeFieldEnd();
    if (this.compression != null) {
      oprot.writeFieldBegin(COMPRESSION_FIELD_DESC);
      oprot.writeString(this.compression);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(IN_MEMORY_FIELD_DESC);
    oprot.writeBool(this.inMemory);
    oprot.writeFieldEnd();
    if (this.bloomFilterType != null) {
      oprot.writeFieldBegin(BLOOM_FILTER_TYPE_FIELD_DESC);
      oprot.writeString(this.bloomFilterType);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(BLOOM_FILTER_VECTOR_SIZE_FIELD_DESC);
    oprot.writeI32(this.bloomFilterVectorSize);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(BLOOM_FILTER_NB_HASHES_FIELD_DESC);
    oprot.writeI32(this.bloomFilterNbHashes);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(BLOCK_CACHE_ENABLED_FIELD_DESC);
    oprot.writeBool(this.blockCacheEnabled);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(TIME_TO_LIVE_FIELD_DESC);
    oprot.writeI32(this.timeToLive);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ColumnDescriptor(");
    boolean first = true;

    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(Bytes.toString(this.name));
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("maxVersions:");
    sb.append(this.maxVersions);
    first = false;
    if (!first) sb.append(", ");
    sb.append("compression:");
    if (this.compression == null) {
      sb.append("null");
    } else {
      sb.append(this.compression);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("inMemory:");
    sb.append(this.inMemory);
    first = false;
    if (!first) sb.append(", ");
    sb.append("bloomFilterType:");
    if (this.bloomFilterType == null) {
      sb.append("null");
    } else {
      sb.append(this.bloomFilterType);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("bloomFilterVectorSize:");
    sb.append(this.bloomFilterVectorSize);
    first = false;
    if (!first) sb.append(", ");
    sb.append("bloomFilterNbHashes:");
    sb.append(this.bloomFilterNbHashes);
    first = false;
    if (!first) sb.append(", ");
    sb.append("blockCacheEnabled:");
    sb.append(this.blockCacheEnabled);
    first = false;
    if (!first) sb.append(", ");
    sb.append("timeToLive:");
    sb.append(this.timeToLive);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}

