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
 * TCell - Used to transport a cell value (byte[]) and the timestamp it was
 * stored with together as a result for get and getRow methods. This promotes
 * the timestamp of a cell to a first-class value, making it easy to take
 * note of temporal data. Cell is used all the way from HStore up to HTable.
 */
public class TCell implements TBase, java.io.Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  private static final TStruct STRUCT_DESC = new TStruct("TCell");
  private static final TField VALUE_FIELD_DESC = new TField("value", TType.STRING, (short)1);
  private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)2);

  public byte[] value;
  public static final int VALUE = 1;
  public long timestamp;
  public static final int TIMESTAMP = 2;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    public boolean timestamp = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(VALUE, new FieldMetaData("value", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
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
    this.__isset.timestamp = true;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TCell(TCell other) {
    if (other.isSetValue()) {
      this.value = other.value;
    }
    __isset.timestamp = other.__isset.timestamp;
    this.timestamp = other.timestamp;
  }

  @Override
  public TCell clone() {
    return new TCell(this);
  }

  public byte[] getValue() {
    return this.value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public void unsetValue() {
    this.value = null;
  }

  // Returns true if field value is set (has been asigned a value) and false otherwise
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

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    this.__isset.timestamp = true;
  }

  public void unsetTimestamp() {
    this.__isset.timestamp = false;
  }

  // Returns true if field timestamp is set (has been asigned a value) and false otherwise
  public boolean isSetTimestamp() {
    return this.__isset.timestamp;
  }

  public void setTimestampIsSet(boolean value) {
    this.__isset.timestamp = value;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
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

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case VALUE:
      return getValue();

    case TIMESTAMP:
      return getTimestamp();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case VALUE:
      return isSetValue();
    case TIMESTAMP:
      return isSetTimestamp();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
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
            this.__isset.timestamp = true;
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
      sb.append(Bytes.toStringBinary(this.value));
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
    // check that fields of type enum have valid values
  }

}

