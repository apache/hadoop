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

import org.apache.commons.lang.builder.HashCodeBuilder;
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
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

/**
 * TCell - Used to transport a cell value (byte[]) and the timestamp it was
 * stored with together as a result for get and getRow methods. This promotes
 * the timestamp of a cell to a first-class value, making it easy to take
 * note of temporal data. Cell is used all the way from HStore up to HTable.
 */
public class TCell implements TBase<TCell._Fields>, java.io.Serializable, Cloneable, Comparable<TCell> {
  private static final TStruct STRUCT_DESC = new TStruct("TCell");

  private static final TField VALUE_FIELD_DESC = new TField("value", TType.STRING, (short)1);
  private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)2);

  public byte[] value;
  public long timestamp;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    VALUE((short)1, "value"),
    TIMESTAMP((short)2, "timestamp");

    private static final Map<Integer, _Fields> byId = new HashMap<Integer, _Fields>();
    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byId.put((int)field._thriftId, field);
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      return byId.get(fieldId);
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
  private static final int __TIMESTAMP_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new EnumMap<_Fields, FieldMetaData>(_Fields.class) {{
    put(_Fields.VALUE, new FieldMetaData("value", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.STRING)));
    put(_Fields.TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.I64)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(TCell.class, metaDataMap);
  }

  public TCell() {
  }

  public TCell(
    byte[] value,
    long timestamp)
  {
    this();
    this.value = value;
    this.timestamp = timestamp;
    setTimestampIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TCell(TCell other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetValue()) {
      this.value = other.value;
    }
    this.timestamp = other.timestamp;
  }

  public TCell deepCopy() {
    return new TCell(this);
  }

  @Deprecated
  public TCell clone() {
    return new TCell(this);
  }

  public byte[] getValue() {
    return this.value;
  }

  public TCell setValue(byte[] value) {
    this.value = value;
    return this;
  }

  public void unsetValue() {
    this.value = null;
  }

  /** Returns true if field value is set (has been asigned a value) and false otherwise */
  public boolean isSetValue() {
    return this.value != null;
  }

  public void setValueIsSet(boolean value) {
    if (!value) {
      this.value = null;
    }
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public TCell setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    setTimestampIsSet(true);
    return this;
  }

  public void unsetTimestamp() {
    __isset_bit_vector.clear(__TIMESTAMP_ISSET_ID);
  }

  /** Returns true if field timestamp is set (has been asigned a value) and false otherwise */
  public boolean isSetTimestamp() {
    return __isset_bit_vector.get(__TIMESTAMP_ISSET_ID);
  }

  public void setTimestampIsSet(boolean value) {
    __isset_bit_vector.set(__TIMESTAMP_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case VALUE:
      if (value == null) {
        unsetValue();
      } else {
        setValue((byte[])value);
      }
      break;

    case TIMESTAMP:
      if (value == null) {
        unsetTimestamp();
      } else {
        setTimestamp((Long)value);
      }
      break;

    }
  }

  public void setFieldValue(int fieldID, Object value) {
    setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case VALUE:
      return getValue();

    case TIMESTAMP:
      return new Long(getTimestamp());

    }
    throw new IllegalStateException();
  }

  public Object getFieldValue(int fieldId) {
    return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    switch (field) {
    case VALUE:
      return isSetValue();
    case TIMESTAMP:
      return isSetTimestamp();
    }
    throw new IllegalStateException();
  }

  public boolean isSet(int fieldID) {
    return isSet(_Fields.findByThriftIdOrThrow(fieldID));
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TCell)
      return this.equals((TCell)that);
    return false;
  }

  public boolean equals(TCell that) {
    if (that == null)
      return false;

    boolean this_present_value = true && this.isSetValue();
    boolean that_present_value = true && that.isSetValue();
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!java.util.Arrays.equals(this.value, that.value))
        return false;
    }

    boolean this_present_timestamp = true;
    boolean that_present_timestamp = true;
    if (this_present_timestamp || that_present_timestamp) {
      if (!(this_present_timestamp && that_present_timestamp))
        return false;
      if (this.timestamp != that.timestamp)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_value = true && (isSetValue());
    builder.append(present_value);
    if (present_value)
      builder.append(value);

    boolean present_timestamp = true;
    builder.append(present_timestamp);
    if (present_timestamp)
      builder.append(timestamp);

    return builder.toHashCode();
  }

  public int compareTo(TCell other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TCell typedOther = (TCell)other;

    lastComparison = Boolean.valueOf(isSetValue()).compareTo(isSetValue());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(value, typedOther.value);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetTimestamp()).compareTo(isSetTimestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(timestamp, typedOther.timestamp);
    if (lastComparison != 0) {
      return lastComparison;
    }
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
      _Fields fieldId = _Fields.findByThriftId(field.id);
      if (fieldId == null) {
        TProtocolUtil.skip(iprot, field.type);
      } else {
        switch (fieldId) {
          case VALUE:
            if (field.type == TType.STRING) {
              this.value = iprot.readBinary();
            } else {
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              setTimestampIsSet(true);
            } else {
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
        }
        iprot.readFieldEnd();
      }
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.value != null) {
      oprot.writeFieldBegin(VALUE_FIELD_DESC);
      oprot.writeBinary(this.value);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
    oprot.writeI64(this.timestamp);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TCell(");
    boolean first = true;

    sb.append("value:");
    if (this.value == null) {
      sb.append("null");
    } else {
      sb.append(this.value);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("timestamp:");
    sb.append(this.timestamp);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}

