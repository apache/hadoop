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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

/**
 * A TRegionInfo contains information about an HTable region.
 */
public class TRegionInfo implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("TRegionInfo");
  private static final TField START_KEY_FIELD_DESC = new TField("startKey", TType.STRING, (short)1);
  private static final TField END_KEY_FIELD_DESC = new TField("endKey", TType.STRING, (short)2);
  private static final TField ID_FIELD_DESC = new TField("id", TType.I64, (short)3);
  private static final TField NAME_FIELD_DESC = new TField("name", TType.STRING, (short)4);
  private static final TField VERSION_FIELD_DESC = new TField("version", TType.BYTE, (short)5);

  public byte[] startKey;
  public static final int STARTKEY = 1;
  public byte[] endKey;
  public static final int ENDKEY = 2;
  public long id;
  public static final int ID = 3;
  public byte[] name;
  public static final int NAME = 4;
  public byte version;
  public static final int VERSION = 5;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean id = false;
    public boolean version = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(STARTKEY, new FieldMetaData("startKey", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(ENDKEY, new FieldMetaData("endKey", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(ID, new FieldMetaData("id", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    put(NAME, new FieldMetaData("name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(VERSION, new FieldMetaData("version", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.BYTE)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(TRegionInfo.class, metaDataMap);
  }

  public TRegionInfo() {
  }

  public TRegionInfo(
    byte[] startKey,
    byte[] endKey,
    long id,
    byte[] name,
    byte version)
  {
    this();
    this.startKey = startKey;
    this.endKey = endKey;
    this.id = id;
    this.__isset.id = true;
    this.name = name;
    this.version = version;
    this.__isset.version = true;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TRegionInfo(TRegionInfo other) {
    if (other.isSetStartKey()) {
      this.startKey = other.startKey;
    }
    if (other.isSetEndKey()) {
      this.endKey = other.endKey;
    }
    __isset.id = other.__isset.id;
    this.id = other.id;
    if (other.isSetName()) {
      this.name = other.name;
    }
    __isset.version = other.__isset.version;
    this.version = other.version;
  }

  @Override
  public TRegionInfo clone() {
    return new TRegionInfo(this);
  }

  public byte[] getStartKey() {
    return this.startKey;
  }

  public void setStartKey(byte[] startKey) {
    this.startKey = startKey;
  }

  public void unsetStartKey() {
    this.startKey = null;
  }

  // Returns true if field startKey is set (has been asigned a value) and false otherwise
  public boolean isSetStartKey() {
    return this.startKey != null;
  }

  public void setStartKeyIsSet(boolean value) {
    if (!value) {
      this.startKey = null;
    }
  }

  public byte[] getEndKey() {
    return this.endKey;
  }

  public void setEndKey(byte[] endKey) {
    this.endKey = endKey;
  }

  public void unsetEndKey() {
    this.endKey = null;
  }

  // Returns true if field endKey is set (has been asigned a value) and false otherwise
  public boolean isSetEndKey() {
    return this.endKey != null;
  }

  public void setEndKeyIsSet(boolean value) {
    if (!value) {
      this.endKey = null;
    }
  }

  public long getId() {
    return this.id;
  }

  public void setId(long id) {
    this.id = id;
    this.__isset.id = true;
  }

  public void unsetId() {
    this.__isset.id = false;
  }

  // Returns true if field id is set (has been asigned a value) and false otherwise
  public boolean isSetId() {
    return this.__isset.id;
  }

  public void setIdIsSet(boolean value) {
    this.__isset.id = value;
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

  public byte getVersion() {
    return this.version;
  }

  public void setVersion(byte version) {
    this.version = version;
    this.__isset.version = true;
  }

  public void unsetVersion() {
    this.__isset.version = false;
  }

  // Returns true if field version is set (has been asigned a value) and false otherwise
  public boolean isSetVersion() {
    return this.__isset.version;
  }

  public void setVersionIsSet(boolean value) {
    this.__isset.version = value;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case STARTKEY:
      if (value == null) {
        unsetStartKey();
      } else {
        setStartKey((byte[])value);
      }
      break;

    case ENDKEY:
      if (value == null) {
        unsetEndKey();
      } else {
        setEndKey((byte[])value);
      }
      break;

    case ID:
      if (value == null) {
        unsetId();
      } else {
        setId((Long)value);
      }
      break;

    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((byte[])value);
      }
      break;

    case VERSION:
      if (value == null) {
        unsetVersion();
      } else {
        setVersion((Byte)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case STARTKEY:
      return getStartKey();

    case ENDKEY:
      return getEndKey();

    case ID:
      return new Long(getId());

    case NAME:
      return getName();

    case VERSION:
      return new Byte(getVersion());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case STARTKEY:
      return isSetStartKey();
    case ENDKEY:
      return isSetEndKey();
    case ID:
      return isSetId();
    case NAME:
      return isSetName();
    case VERSION:
      return isSetVersion();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TRegionInfo)
      return this.equals((TRegionInfo)that);
    return false;
  }

  public boolean equals(TRegionInfo that) {
    if (that == null)
      return false;

    boolean this_present_startKey = true && this.isSetStartKey();
    boolean that_present_startKey = true && that.isSetStartKey();
    if (this_present_startKey || that_present_startKey) {
      if (!(this_present_startKey && that_present_startKey))
        return false;
      if (!java.util.Arrays.equals(this.startKey, that.startKey))
        return false;
    }

    boolean this_present_endKey = true && this.isSetEndKey();
    boolean that_present_endKey = true && that.isSetEndKey();
    if (this_present_endKey || that_present_endKey) {
      if (!(this_present_endKey && that_present_endKey))
        return false;
      if (!java.util.Arrays.equals(this.endKey, that.endKey))
        return false;
    }

    boolean this_present_id = true;
    boolean that_present_id = true;
    if (this_present_id || that_present_id) {
      if (!(this_present_id && that_present_id))
        return false;
      if (this.id != that.id)
        return false;
    }

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!java.util.Arrays.equals(this.name, that.name))
        return false;
    }

    boolean this_present_version = true;
    boolean that_present_version = true;
    if (this_present_version || that_present_version) {
      if (!(this_present_version && that_present_version))
        return false;
      if (this.version != that.version)
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
        case STARTKEY:
          if (field.type == TType.STRING) {
            this.startKey = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case ENDKEY:
          if (field.type == TType.STRING) {
            this.endKey = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case ID:
          if (field.type == TType.I64) {
            this.id = iprot.readI64();
            this.__isset.id = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case NAME:
          if (field.type == TType.STRING) {
            this.name = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case VERSION:
          if (field.type == TType.BYTE) {
            this.version = iprot.readByte();
            this.__isset.version = true;
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
    if (this.startKey != null) {
      oprot.writeFieldBegin(START_KEY_FIELD_DESC);
      oprot.writeBinary(this.startKey);
      oprot.writeFieldEnd();
    }
    if (this.endKey != null) {
      oprot.writeFieldBegin(END_KEY_FIELD_DESC);
      oprot.writeBinary(this.endKey);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(ID_FIELD_DESC);
    oprot.writeI64(this.id);
    oprot.writeFieldEnd();
    if (this.name != null) {
      oprot.writeFieldBegin(NAME_FIELD_DESC);
      oprot.writeBinary(this.name);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(VERSION_FIELD_DESC);
    oprot.writeByte(this.version);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TRegionInfo(");
    boolean first = true;

    sb.append("startKey:");
    if (this.startKey == null) {
      sb.append("null");
    } else {
      sb.append(this.startKey);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("endKey:");
    if (this.endKey == null) {
      sb.append("null");
    } else {
      sb.append(this.endKey);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("id:");
    sb.append(this.id);
    first = false;
    if (!first) sb.append(", ");
    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("version:");
    sb.append(this.version);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}

