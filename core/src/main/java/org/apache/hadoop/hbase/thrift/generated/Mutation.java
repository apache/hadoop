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
 * A Mutation object is used to either update or delete a column-value.
 */
public class Mutation implements TBase<Mutation._Fields>, java.io.Serializable, Cloneable, Comparable<Mutation> {
  private static final TStruct STRUCT_DESC = new TStruct("Mutation");

  private static final TField IS_DELETE_FIELD_DESC = new TField("isDelete", TType.BOOL, (short)1);
  private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)2);
  private static final TField VALUE_FIELD_DESC = new TField("value", TType.STRING, (short)3);

  public boolean isDelete;
  public byte[] column;
  public byte[] value;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    IS_DELETE((short)1, "isDelete"),
    COLUMN((short)2, "column"),
    VALUE((short)3, "value");

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
  private static final int __ISDELETE_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new EnumMap<_Fields, FieldMetaData>(_Fields.class) {{
    put(_Fields.IS_DELETE, new FieldMetaData("isDelete", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.BOOL)));
    put(_Fields.COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.STRING)));
    put(_Fields.VALUE, new FieldMetaData("value", TFieldRequirementType.DEFAULT,
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
    setIsDeleteIsSet(true);
    this.column = column;
    this.value = value;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Mutation(Mutation other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.isDelete = other.isDelete;
    if (other.isSetColumn()) {
      this.column = other.column;
    }
    if (other.isSetValue()) {
      this.value = other.value;
    }
  }

  public Mutation deepCopy() {
    return new Mutation(this);
  }

  @Deprecated
  public Mutation clone() {
    return new Mutation(this);
  }

  public boolean isIsDelete() {
    return this.isDelete;
  }

  public Mutation setIsDelete(boolean isDelete) {
    this.isDelete = isDelete;
    setIsDeleteIsSet(true);
    return this;
  }

  public void unsetIsDelete() {
    __isset_bit_vector.clear(__ISDELETE_ISSET_ID);
  }

  /** Returns true if field isDelete is set (has been asigned a value) and false otherwise */
  public boolean isSetIsDelete() {
    return __isset_bit_vector.get(__ISDELETE_ISSET_ID);
  }

  public void setIsDeleteIsSet(boolean value) {
    __isset_bit_vector.set(__ISDELETE_ISSET_ID, value);
  }

  public byte[] getColumn() {
    return this.column;
  }

  public Mutation setColumn(byte[] column) {
    this.column = column;
    return this;
  }

  public void unsetColumn() {
    this.column = null;
  }

  /** Returns true if field column is set (has been asigned a value) and false otherwise */
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

  public Mutation setValue(byte[] value) {
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

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case IS_DELETE:
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

    }
  }

  public void setFieldValue(int fieldID, Object value) {
    setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case IS_DELETE:
      return new Boolean(isIsDelete());

    case COLUMN:
      return getColumn();

    case VALUE:
      return getValue();

    }
    throw new IllegalStateException();
  }

  public Object getFieldValue(int fieldId) {
    return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    switch (field) {
    case IS_DELETE:
      return isSetIsDelete();
    case COLUMN:
      return isSetColumn();
    case VALUE:
      return isSetValue();
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
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_isDelete = true;
    builder.append(present_isDelete);
    if (present_isDelete)
      builder.append(isDelete);

    boolean present_column = true && (isSetColumn());
    builder.append(present_column);
    if (present_column)
      builder.append(column);

    boolean present_value = true && (isSetValue());
    builder.append(present_value);
    if (present_value)
      builder.append(value);

    return builder.toHashCode();
  }

  public int compareTo(Mutation other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Mutation typedOther = (Mutation)other;

    lastComparison = Boolean.valueOf(isSetIsDelete()).compareTo(isSetIsDelete());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(isDelete, typedOther.isDelete);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetColumn()).compareTo(isSetColumn());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(column, typedOther.column);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetValue()).compareTo(isSetValue());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(value, typedOther.value);
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
          case IS_DELETE:
            if (field.type == TType.BOOL) {
              this.isDelete = iprot.readBool();
              setIsDeleteIsSet(true);
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
  }

}

