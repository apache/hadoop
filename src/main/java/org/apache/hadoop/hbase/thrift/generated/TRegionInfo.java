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
package org.apache.hadoop.hbase.thrift.generated;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

/**
 * A TRegionInfo contains information about an HTable region.
 */
public class TRegionInfo implements TBase<TRegionInfo, TRegionInfo._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("TRegionInfo");

  private static final TField START_KEY_FIELD_DESC = new TField("startKey", TType.STRING, (short)1);
  private static final TField END_KEY_FIELD_DESC = new TField("endKey", TType.STRING, (short)2);
  private static final TField ID_FIELD_DESC = new TField("id", TType.I64, (short)3);
  private static final TField NAME_FIELD_DESC = new TField("name", TType.STRING, (short)4);
  private static final TField VERSION_FIELD_DESC = new TField("version", TType.BYTE, (short)5);

  public ByteBuffer startKey;
  public ByteBuffer endKey;
  public long id;
  public ByteBuffer name;
  public byte version;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    START_KEY((short)1, "startKey"),
    END_KEY((short)2, "endKey"),
    ID((short)3, "id"),
    NAME((short)4, "name"),
    VERSION((short)5, "version");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // START_KEY
          return START_KEY;
        case 2: // END_KEY
          return END_KEY;
        case 3: // ID
          return ID;
        case 4: // NAME
          return NAME;
        case 5: // VERSION
          return VERSION;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __ID_ISSET_ID = 0;
  private static final int __VERSION_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.START_KEY, new FieldMetaData("startKey", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.STRING        , "Text")));
    tmpMap.put(_Fields.END_KEY, new FieldMetaData("endKey", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.STRING        , "Text")));
    tmpMap.put(_Fields.ID, new FieldMetaData("id", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.I64)));
    tmpMap.put(_Fields.NAME, new FieldMetaData("name", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.STRING        , "Text")));
    tmpMap.put(_Fields.VERSION, new FieldMetaData("version", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.BYTE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(TRegionInfo.class, metaDataMap);
  }

  public TRegionInfo() {
  }

  public TRegionInfo(
    ByteBuffer startKey,
    ByteBuffer endKey,
    long id,
    ByteBuffer name,
    byte version)
  {
    this();
    this.startKey = startKey;
    this.endKey = endKey;
    this.id = id;
    setIdIsSet(true);
    this.name = name;
    this.version = version;
    setVersionIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TRegionInfo(TRegionInfo other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetStartKey()) {
      this.startKey = other.startKey;
    }
    if (other.isSetEndKey()) {
      this.endKey = other.endKey;
    }
    this.id = other.id;
    if (other.isSetName()) {
      this.name = other.name;
    }
    this.version = other.version;
  }

  public TRegionInfo deepCopy() {
    return new TRegionInfo(this);
  }

  @Override
  public void clear() {
    this.startKey = null;
    this.endKey = null;
    setIdIsSet(false);
    this.id = 0;
    this.name = null;
    setVersionIsSet(false);
    this.version = 0;
  }

  public byte[] getStartKey() {
    setStartKey(TBaseHelper.rightSize(startKey));
    return startKey.array();
  }

  public ByteBuffer BufferForStartKey() {
    return startKey;
  }

  public TRegionInfo setStartKey(byte[] startKey) {
    setStartKey(ByteBuffer.wrap(startKey));
    return this;
  }

  public TRegionInfo setStartKey(ByteBuffer startKey) {
    this.startKey = startKey;
    return this;
  }

  public void unsetStartKey() {
    this.startKey = null;
  }

  /** Returns true if field startKey is set (has been asigned a value) and false otherwise */
  public boolean isSetStartKey() {
    return this.startKey != null;
  }

  public void setStartKeyIsSet(boolean value) {
    if (!value) {
      this.startKey = null;
    }
  }

  public byte[] getEndKey() {
    setEndKey(TBaseHelper.rightSize(endKey));
    return endKey.array();
  }

  public ByteBuffer BufferForEndKey() {
    return endKey;
  }

  public TRegionInfo setEndKey(byte[] endKey) {
    setEndKey(ByteBuffer.wrap(endKey));
    return this;
  }

  public TRegionInfo setEndKey(ByteBuffer endKey) {
    this.endKey = endKey;
    return this;
  }

  public void unsetEndKey() {
    this.endKey = null;
  }

  /** Returns true if field endKey is set (has been asigned a value) and false otherwise */
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

  public TRegionInfo setId(long id) {
    this.id = id;
    setIdIsSet(true);
    return this;
  }

  public void unsetId() {
    __isset_bit_vector.clear(__ID_ISSET_ID);
  }

  /** Returns true if field id is set (has been asigned a value) and false otherwise */
  public boolean isSetId() {
    return __isset_bit_vector.get(__ID_ISSET_ID);
  }

  public void setIdIsSet(boolean value) {
    __isset_bit_vector.set(__ID_ISSET_ID, value);
  }

  public byte[] getName() {
    setName(TBaseHelper.rightSize(name));
    return name.array();
  }

  public ByteBuffer BufferForName() {
    return name;
  }

  public TRegionInfo setName(byte[] name) {
    setName(ByteBuffer.wrap(name));
    return this;
  }

  public TRegionInfo setName(ByteBuffer name) {
    this.name = name;
    return this;
  }

  public void unsetName() {
    this.name = null;
  }

  /** Returns true if field name is set (has been asigned a value) and false otherwise */
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

  public TRegionInfo setVersion(byte version) {
    this.version = version;
    setVersionIsSet(true);
    return this;
  }

  public void unsetVersion() {
    __isset_bit_vector.clear(__VERSION_ISSET_ID);
  }

  /** Returns true if field version is set (has been asigned a value) and false otherwise */
  public boolean isSetVersion() {
    return __isset_bit_vector.get(__VERSION_ISSET_ID);
  }

  public void setVersionIsSet(boolean value) {
    __isset_bit_vector.set(__VERSION_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case START_KEY:
      if (value == null) {
        unsetStartKey();
      } else {
        setStartKey((ByteBuffer)value);
      }
      break;

    case END_KEY:
      if (value == null) {
        unsetEndKey();
      } else {
        setEndKey((ByteBuffer)value);
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
        setName((ByteBuffer)value);
      }
      break;

    case VERSION:
      if (value == null) {
        unsetVersion();
      } else {
        setVersion((Byte)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case START_KEY:
      return getStartKey();

    case END_KEY:
      return getEndKey();

    case ID:
      return new Long(getId());

    case NAME:
      return getName();

    case VERSION:
      return new Byte(getVersion());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case START_KEY:
      return isSetStartKey();
    case END_KEY:
      return isSetEndKey();
    case ID:
      return isSetId();
    case NAME:
      return isSetName();
    case VERSION:
      return isSetVersion();
    }
    throw new IllegalStateException();
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
      if (!this.startKey.equals(that.startKey))
        return false;
    }

    boolean this_present_endKey = true && this.isSetEndKey();
    boolean that_present_endKey = true && that.isSetEndKey();
    if (this_present_endKey || that_present_endKey) {
      if (!(this_present_endKey && that_present_endKey))
        return false;
      if (!this.endKey.equals(that.endKey))
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
      if (!this.name.equals(that.name))
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

  public int compareTo(TRegionInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TRegionInfo typedOther = (TRegionInfo)other;

    lastComparison = Boolean.valueOf(isSetStartKey()).compareTo(typedOther.isSetStartKey());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartKey()) {
      lastComparison = TBaseHelper.compareTo(this.startKey, typedOther.startKey);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEndKey()).compareTo(typedOther.isSetEndKey());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEndKey()) {
      lastComparison = TBaseHelper.compareTo(this.endKey, typedOther.endKey);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetId()).compareTo(typedOther.isSetId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetId()) {
      lastComparison = TBaseHelper.compareTo(this.id, typedOther.id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetName()).compareTo(typedOther.isSetName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetName()) {
      lastComparison = TBaseHelper.compareTo(this.name, typedOther.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetVersion()).compareTo(typedOther.isSetVersion());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVersion()) {
      lastComparison = TBaseHelper.compareTo(this.version, typedOther.version);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
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
      switch (field.id) {
        case 1: // START_KEY
          if (field.type == TType.STRING) {
            this.startKey = iprot.readBinary();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // END_KEY
          if (field.type == TType.STRING) {
            this.endKey = iprot.readBinary();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // ID
          if (field.type == TType.I64) {
            this.id = iprot.readI64();
            setIdIsSet(true);
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 4: // NAME
          if (field.type == TType.STRING) {
            this.name = iprot.readBinary();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 5: // VERSION
          if (field.type == TType.BYTE) {
            this.version = iprot.readByte();
            setVersionIsSet(true);
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
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
  }

}

