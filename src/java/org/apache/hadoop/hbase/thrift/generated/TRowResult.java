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
 * Holds row name and then a map of columns to cells.
 */
public class TRowResult implements TBase, java.io.Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  private static final TStruct STRUCT_DESC = new TStruct("TRowResult");
  private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)1);
  private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.MAP, (short)2);

  public byte[] row;
  public static final int ROW = 1;
  public Map<byte[],TCell> columns;
  public static final int COLUMNS = 2;

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
        new MapMetaData(TType.MAP, 
            new FieldValueMetaData(TType.STRING), 
            new StructMetaData(TType.STRUCT, TCell.class))));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(TRowResult.class, metaDataMap);
  }

  public TRowResult() {
  }

  public TRowResult(
    byte[] row,
    Map<byte[],TCell> columns)
  {
    this();
    this.row = row;
    this.columns = columns;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TRowResult(TRowResult other) {
    if (other.isSetRow()) {
      this.row = other.row;
    }
    if (other.isSetColumns()) {
      Map<byte[],TCell> __this__columns = new HashMap<byte[],TCell>();
      for (Map.Entry<byte[], TCell> other_element : other.columns.entrySet()) {

        byte[] other_element_key = other_element.getKey();
        TCell other_element_value = other_element.getValue();

        byte[] __this__columns_copy_key = other_element_key;

        TCell __this__columns_copy_value = new TCell(other_element_value);

        __this__columns.put(__this__columns_copy_key, __this__columns_copy_value);
      }
      this.columns = __this__columns;
    }
  }

  @Override
  public TRowResult clone() {
    return new TRowResult(this);
  }

  public byte[] getRow() {
    return this.row;
  }

  public void setRow(byte[] row) {
    this.row = row;
  }

  public void unsetRow() {
    this.row = null;
  }

  // Returns true if field row is set (has been asigned a value) and false otherwise
  public boolean isSetRow() {
    return this.row != null;
  }

  public void setRowIsSet(boolean value) {
    if (!value) {
      this.row = null;
    }
  }

  public int getColumnsSize() {
    return (this.columns == null) ? 0 : this.columns.size();
  }

  public void putToColumns(byte[] key, TCell val) {
    if (this.columns == null) {
      this.columns = new HashMap<byte[],TCell>();
    }
    this.columns.put(key, val);
  }

  public Map<byte[],TCell> getColumns() {
    return this.columns;
  }

  public void setColumns(Map<byte[],TCell> columns) {
    this.columns = columns;
  }

  public void unsetColumns() {
    this.columns = null;
  }

  // Returns true if field columns is set (has been asigned a value) and false otherwise
  public boolean isSetColumns() {
    return this.columns != null;
  }

  public void setColumnsIsSet(boolean value) {
    if (!value) {
      this.columns = null;
    }
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case ROW:
      if (value == null) {
        unsetRow();
      } else {
        setRow((byte[])value);
      }
      break;

    case COLUMNS:
      if (value == null) {
        unsetColumns();
      } else {
        setColumns((Map<byte[],TCell>)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case ROW:
      return getRow();

    case COLUMNS:
      return getColumns();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case ROW:
      return isSetRow();
    case COLUMNS:
      return isSetColumns();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TRowResult)
      return this.equals((TRowResult)that);
    return false;
  }

  public boolean equals(TRowResult that) {
    if (that == null)
      return false;

    boolean this_present_row = true && this.isSetRow();
    boolean that_present_row = true && that.isSetRow();
    if (this_present_row || that_present_row) {
      if (!(this_present_row && that_present_row))
        return false;
      if (!java.util.Arrays.equals(this.row, that.row))
        return false;
    }

    boolean this_present_columns = true && this.isSetColumns();
    boolean that_present_columns = true && that.isSetColumns();
    if (this_present_columns || that_present_columns) {
      if (!(this_present_columns && that_present_columns))
        return false;
      if (!this.columns.equals(that.columns))
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
        case ROW:
          if (field.type == TType.STRING) {
            this.row = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case COLUMNS:
          if (field.type == TType.MAP) {
            {
              TMap _map4 = iprot.readMapBegin();
              this.columns = new HashMap<byte[],TCell>(2*_map4.size);
              for (int _i5 = 0; _i5 < _map4.size; ++_i5)
              {
                byte[] _key6;
                TCell _val7;
                _key6 = iprot.readBinary();
                _val7 = new TCell();
                _val7.read(iprot);
                this.columns.put(_key6, _val7);
              }
              iprot.readMapEnd();
            }
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
    if (this.row != null) {
      oprot.writeFieldBegin(ROW_FIELD_DESC);
      oprot.writeBinary(this.row);
      oprot.writeFieldEnd();
    }
    if (this.columns != null) {
      oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
      {
        oprot.writeMapBegin(new TMap(TType.STRING, TType.STRUCT, this.columns.size()));
        for (Map.Entry<byte[], TCell> _iter8 : this.columns.entrySet())        {
          oprot.writeBinary(_iter8.getKey());
          _iter8.getValue().write(oprot);
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TRowResult(");
    boolean first = true;

    sb.append("row:");
    if (this.row == null) {
      sb.append("null");
    } else {
      sb.append(Bytes.toStringBinary(this.row));
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("columns:");
    if (this.columns == null) {
      sb.append("null");
    } else {
      sb.append(this.columns);
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

