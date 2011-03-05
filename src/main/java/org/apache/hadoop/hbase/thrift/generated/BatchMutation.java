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
 * A BatchMutation object is used to apply a number of Mutations to a single row.
 */
public class BatchMutation implements TBase<BatchMutation, BatchMutation._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("BatchMutation");

  private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)1);
  private static final TField MUTATIONS_FIELD_DESC = new TField("mutations", TType.LIST, (short)2);

  public ByteBuffer row;
  public List<Mutation> mutations;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    ROW((short)1, "row"),
    MUTATIONS((short)2, "mutations");

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
        case 1: // ROW
          return ROW;
        case 2: // MUTATIONS
          return MUTATIONS;
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

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT,
        new FieldValueMetaData(TType.STRING        , "Text")));
    tmpMap.put(_Fields.MUTATIONS, new FieldMetaData("mutations", TFieldRequirementType.DEFAULT,
        new ListMetaData(TType.LIST,
            new StructMetaData(TType.STRUCT, Mutation.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(BatchMutation.class, metaDataMap);
  }

  public BatchMutation() {
  }

  public BatchMutation(
    ByteBuffer row,
    List<Mutation> mutations)
  {
    this();
    this.row = row;
    this.mutations = mutations;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public BatchMutation(BatchMutation other) {
    if (other.isSetRow()) {
      this.row = other.row;
    }
    if (other.isSetMutations()) {
      List<Mutation> __this__mutations = new ArrayList<Mutation>();
      for (Mutation other_element : other.mutations) {
        __this__mutations.add(new Mutation(other_element));
      }
      this.mutations = __this__mutations;
    }
  }

  public BatchMutation deepCopy() {
    return new BatchMutation(this);
  }

  @Override
  public void clear() {
    this.row = null;
    this.mutations = null;
  }

  public byte[] getRow() {
    setRow(TBaseHelper.rightSize(row));
    return row.array();
  }

  public ByteBuffer BufferForRow() {
    return row;
  }

  public BatchMutation setRow(byte[] row) {
    setRow(ByteBuffer.wrap(row));
    return this;
  }

  public BatchMutation setRow(ByteBuffer row) {
    this.row = row;
    return this;
  }

  public void unsetRow() {
    this.row = null;
  }

  /** Returns true if field row is set (has been asigned a value) and false otherwise */
  public boolean isSetRow() {
    return this.row != null;
  }

  public void setRowIsSet(boolean value) {
    if (!value) {
      this.row = null;
    }
  }

  public int getMutationsSize() {
    return (this.mutations == null) ? 0 : this.mutations.size();
  }

  public java.util.Iterator<Mutation> getMutationsIterator() {
    return (this.mutations == null) ? null : this.mutations.iterator();
  }

  public void addToMutations(Mutation elem) {
    if (this.mutations == null) {
      this.mutations = new ArrayList<Mutation>();
    }
    this.mutations.add(elem);
  }

  public List<Mutation> getMutations() {
    return this.mutations;
  }

  public BatchMutation setMutations(List<Mutation> mutations) {
    this.mutations = mutations;
    return this;
  }

  public void unsetMutations() {
    this.mutations = null;
  }

  /** Returns true if field mutations is set (has been asigned a value) and false otherwise */
  public boolean isSetMutations() {
    return this.mutations != null;
  }

  public void setMutationsIsSet(boolean value) {
    if (!value) {
      this.mutations = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ROW:
      if (value == null) {
        unsetRow();
      } else {
        setRow((ByteBuffer)value);
      }
      break;

    case MUTATIONS:
      if (value == null) {
        unsetMutations();
      } else {
        setMutations((List<Mutation>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ROW:
      return getRow();

    case MUTATIONS:
      return getMutations();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ROW:
      return isSetRow();
    case MUTATIONS:
      return isSetMutations();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof BatchMutation)
      return this.equals((BatchMutation)that);
    return false;
  }

  public boolean equals(BatchMutation that) {
    if (that == null)
      return false;

    boolean this_present_row = true && this.isSetRow();
    boolean that_present_row = true && that.isSetRow();
    if (this_present_row || that_present_row) {
      if (!(this_present_row && that_present_row))
        return false;
      if (!this.row.equals(that.row))
        return false;
    }

    boolean this_present_mutations = true && this.isSetMutations();
    boolean that_present_mutations = true && that.isSetMutations();
    if (this_present_mutations || that_present_mutations) {
      if (!(this_present_mutations && that_present_mutations))
        return false;
      if (!this.mutations.equals(that.mutations))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(BatchMutation other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    BatchMutation typedOther = (BatchMutation)other;

    lastComparison = Boolean.valueOf(isSetRow()).compareTo(typedOther.isSetRow());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRow()) {
      lastComparison = TBaseHelper.compareTo(this.row, typedOther.row);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMutations()).compareTo(typedOther.isSetMutations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMutations()) {
      lastComparison = TBaseHelper.compareTo(this.mutations, typedOther.mutations);
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
        case 1: // ROW
          if (field.type == TType.STRING) {
            this.row = iprot.readBinary();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // MUTATIONS
          if (field.type == TType.LIST) {
            {
              TList _list0 = iprot.readListBegin();
              this.mutations = new ArrayList<Mutation>(_list0.size);
              for (int _i1 = 0; _i1 < _list0.size; ++_i1)
              {
                Mutation _elem2;
                _elem2 = new Mutation();
                _elem2.read(iprot);
                this.mutations.add(_elem2);
              }
              iprot.readListEnd();
            }
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
    if (this.row != null) {
      oprot.writeFieldBegin(ROW_FIELD_DESC);
      oprot.writeBinary(this.row);
      oprot.writeFieldEnd();
    }
    if (this.mutations != null) {
      oprot.writeFieldBegin(MUTATIONS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRUCT, this.mutations.size()));
        for (Mutation _iter3 : this.mutations)
        {
          _iter3.write(oprot);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("BatchMutation(");
    boolean first = true;

    sb.append("row:");
    if (this.row == null) {
      sb.append("null");
    } else {
      sb.append(this.row);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("mutations:");
    if (this.mutations == null) {
      sb.append("null");
    } else {
      sb.append(this.mutations);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}

