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
 * A Mutation object is used to either update or delete a column-value.
 */
public class Mutation implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("Mutation");
  private static final TField IS_DELETE_FIELD_DESC = new TField("isDelete", TType.BOOL, (short)1);
  private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)2);
  private static final TField VALUE_FIELD_DESC = new TField("value", TType.STRING, (short)3);

  public boolean isDelete;
  public static final int ISDELETE = 1;
  public byte[] column;
  public static final int COLUMN = 2;
  public byte[] value;
  public static final int VALUE = 3;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean isDelete = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(ISDELETE, new FieldMetaData("isDelete", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.BOOL)));
    put(COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(VALUE, new FieldMetaData("value", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(Mutation.class, metaDataMap);
  }

  public Mutation() {
    this.isDelete = false;

  }

  public Mutation(
    boolean isDelete,
    byte[] column,
    byte[] value)
  {
    this();
    this.isDelete = isDelete;
    this.__isset.isDelete = true;
    this.column = column;
    this.value = value;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Mutation(Mutation other) {
    __isset.isDelete = other.__isset.isDelete;
    this.isDelete = other.isDelete;
    if (other.isSetColumn()) {
      this.column = other.column;
    }
    if (other.isSetValue()) {
      this.value = other.value;
    }
  }

  @Override
  public Mutation clone() {
    return new Mutation(this);
  }

  public boolean isIsDelete() {
    return this.isDelete;
  }

  public void setIsDelete(boolean isDelete) {
    this.isDelete = isDelete;
    this.__isset.isDelete = true;
  }

  public void unsetIsDelete() {
    this.__isset.isDelete = false;
  }

  // Returns true if field isDelete is set (has been asigned a value) and false otherwise
  public boolean isSetIsDelete() {
    return this.__isset.isDelete;
  }

  public void setIsDeleteIsSet(boolean value) {
    this.__isset.isDelete = value;
  }

  public byte[] getColumn() {
    return this.column;
  }

  public void setColumn(byte[] column) {
    this.column = column;
  }

  public void unsetColumn() {
    this.column = null;
  }

  // Returns true if field column is set (has been asigned a value) and false otherwise
  public boolean isSetColumn() {
    return this.column != null;
  }

  public void setColumnIsSet(boolean value) {
    if (!value) {
      this.column = null;
    }
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

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case ISDELETE:
      if (value == null) {
        unsetIsDelete();
      } else {
        setIsDelete((Boolean)value);
      }
      break;

    case COLUMN:
      if (value == null) {
        unsetColumn();
      } else {
        setColumn((byte[])value);
      }
      break;

    case VALUE:
      if (value == null) {
        unsetValue();
      } else {
        setValue((byte[])value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case ISDELETE:
      return new Boolean(isIsDelete());

    case COLUMN:
      return getColumn();

    case VALUE:
      return getValue();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case ISDELETE:
      return isSetIsDelete();
    case COLUMN:
      return isSetColumn();
    case VALUE:
      return isSetValue();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Mutation)
      return this.equals((Mutation)that);
    return false;
  }

  public boolean equals(Mutation that) {
    if (that == null)
      return false;

    boolean this_present_isDelete = true;
    boolean that_present_isDelete = true;
    if (this_present_isDelete || that_present_isDelete) {
      if (!(this_present_isDelete && that_present_isDelete))
        return false;
      if (this.isDelete != that.isDelete)
        return false;
    }

    boolean this_present_column = true && this.isSetColumn();
    boolean that_present_column = true && that.isSetColumn();
    if (this_present_column || that_present_column) {
      if (!(this_present_column && that_present_column))
        return false;
      if (!java.util.Arrays.equals(this.column, that.column))
        return false;
    }

    boolean this_present_value = true && this.isSetValue();
    boolean that_present_value = true && that.isSetValue();
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!java.util.Arrays.equals(this.value, that.value))
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
        case ISDELETE:
          if (field.type == TType.BOOL) {
            this.isDelete = iprot.readBool();
            this.__isset.isDelete = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case COLUMN:
          if (field.type == TType.STRING) {
            this.column = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case VALUE:
          if (field.type == TType.STRING) {
            this.value = iprot.readBinary();
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
    oprot.writeFieldBegin(IS_DELETE_FIELD_DESC);
    oprot.writeBool(this.isDelete);
    oprot.writeFieldEnd();
    if (this.column != null) {
      oprot.writeFieldBegin(COLUMN_FIELD_DESC);
      oprot.writeBinary(this.column);
      oprot.writeFieldEnd();
    }
    if (this.value != null) {
      oprot.writeFieldBegin(VALUE_FIELD_DESC);
      oprot.writeBinary(this.value);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Mutation(");
    boolean first = true;

    sb.append("isDelete:");
    sb.append(this.isDelete);
    first = false;
    if (!first) sb.append(", ");
    sb.append("column:");
    if (this.column == null) {
      sb.append("null");
    } else {
      sb.append(this.column);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("value:");
    if (this.value == null) {
      sb.append("null");
    } else {
      sb.append(this.value);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}

