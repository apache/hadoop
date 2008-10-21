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

package org.apache.hadoop.hive.serde.simple_meta;

import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.serde.thrift.*;
import com.facebook.thrift.TException;
import com.facebook.thrift.TBase;
import com.facebook.thrift.TSerializer;
import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;



public class MetadataTypedColumnsetSerDe  extends ByteStreamTypedSerDe implements SerDe {

  protected TIOStreamTransport outTransport, inTransport;
  protected TProtocol outProtocol, inProtocol;

  final public static String DefaultSeparator = "\001";

  protected boolean inStreaming;
  private String separator;
  // constant for now, will make it configurable later.
  private String nullString = "\\N"; 
  private ColumnSet cachedObj;

  // stores the column name and its position in the input
  private HashMap<String,java.lang.Integer> _columns;

  // stores the columns in order
  private String _columns_list[];

  public String toString() {
    return "MetaDataTypedColumnsetSerDe[" + separator + "," + _columns + "]";
  }

  public MetadataTypedColumnsetSerDe() throws SerDeException {
    this(org.apache.hadoop.hive.serde.ColumnSet.class);
  }

  public MetadataTypedColumnsetSerDe (Class<?> argType) throws SerDeException {
    this(argType, new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory());
  }

  public MetadataTypedColumnsetSerDe (Class<?> argType, TProtocolFactory inFactory,
                                      TProtocolFactory outFactory) throws SerDeException {
    super(argType);
    cachedObj = new ColumnSet();
    cachedObj.col = new ArrayList<String>();
    separator = DefaultSeparator;
    outTransport = new TIOStreamTransport(bos);
    inTransport = new TIOStreamTransport(bis);
    outProtocol = outFactory.getProtocol(outTransport);
    inProtocol = inFactory.getProtocol(inTransport);
    json_serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
  }

  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    inStreaming = job.get("hive.streaming.select") != null;
    separator = DefaultSeparator;
    String alt_sep = tbl.getProperty(Constants.SERIALIZATION_FORMAT);
    if(alt_sep != null && alt_sep.length() > 0) {
      try {
        byte b [] = new byte[1];
        b[0] = Byte.valueOf(alt_sep).byteValue();
        separator = new String(b);
      } catch(NumberFormatException e) {
        separator = alt_sep;
      }
    }
    _columns_list = tbl.getProperty("columns").split(",");
    _columns = new HashMap<String, java.lang.Integer>();
    int index = 0;
    for(String column: _columns_list) {
      _columns.put(column, index);
      index++;
    }
  }

  public static Object deserialize(ColumnSet c, String row, String sep, String nullString) throws Exception {
    c.col.clear();
    String [] l1 = row.split(sep, -1);

    for(String s: l1) {
      if (s.equals(nullString)) {
        c.col.add(null);
      } else {
        c.col.add(s);
      }
    }
    return (c);
  }

  public Object deserialize(Writable field) throws SerDeException {

    /*
      OK - if we had the runtime thrift here, we'd have the ddl in the serde and
      just call runtime_thrift.deserialize(this.ddl,field);

      pw 2/5/08

    */

    ColumnSet c = cachedObj;
    try {
      try {
        Text tw = (Text)field;
        String row = tw.toString();
        return(deserialize(c, row, separator, nullString));
      } catch (ClassCastException e) {
        throw new SerDeException("columnsetSerDe  expects Text", e);
      } catch (Exception e) {
        throw new SerDeException(e);
      }
    } catch (SerDeException e) {
      // don't want to crap out streaming jobs because of one error.
      if(inStreaming) {
        return (c);
      } else {
        throw (e);
      }
    }
  }
  public Writable serialize(Object obj) throws SerDeException {
    // Do type conversion if necessary
    ColumnSet c = null;
    if (obj instanceof ColumnSet) {
      c = (ColumnSet) obj;
    } else if (obj instanceof List) {
      ArrayList<String> a = new ArrayList<String>();
      // convert all obj to string
      for(Object o: (List<?>)obj) {
        a.add(o == null ? nullString : o.toString());
      }
      c = new ColumnSet(a);
    } else {
      throw new SerDeException( this.getClass().getName() + " can only serialize ColumnSet or java List.");
    }

    // Serialize columnSet
    StringBuilder sb = new StringBuilder();
    for(int i=0; i<c.col.size(); i++) {
      if (i>0) sb.append(separator);
      sb.append(c.col.get(i));
    }
    return new Text(sb.toString());
  }

  public SerDeField getFieldFromExpression(SerDeField parentField, String fieldExpression) throws SerDeException {


    /* question:
    //  should we have a HiveType ??

    class HiveType {

    public HiveType() { }
    public HiveType getTypeFromExpression(fieldExpression);
    public HiveType [] getSubtypes();

    // does the below make sense ? where else would the hivefield be found for this type
    public SerDeField getSerDeField( ) { }
    // problem is if this isn't some base type or collection of base type...

    // or should someone be able to do select u.affiliations_subtable from u insert into just_affiliations_subtable
    // maybe ...

    // But, how at runtime can i do a get of an affiliations_subtable Object in Java???
    // The question is what is this used for. If by streaming, I just need a way of serlializing it
    // to stream it into the stream.

    // Another question is could we take advantage so we can supply Python with this.

    // what about a python constructed class for reading/writing these types? If we had the runtime thing, no problem
    // at all.

    // What is the reason for not keeping it in the SerDe?  Just makes sense to have the following abstractions:

    // 1. RosettaObject(implements Writable)  - to get and set data. Could be a full row or subtype

    // one question is serialize/deserialize - we presumably do this once on the entire object, and
    // then it gets passed to us

    // 2. RosettaType - represents a "schema" / type
    // 3. RosettaSerDe - thin wrapper around the other 2
    // 4. RosettaField implements SerDeField

    The SerDe should return an overall RosettaObject which is then passed to the various RosettaField get
    methods.

    // Looking at SerDe, we need to implement:
    1. serialize
    2. deserialize
    3. getFields
    4. getFieldFromExpression
    5. toJSONString

    IMPLEMENTATIONS:

    1. serialize -
    SDObject obj = new SDObject(tableOrColumnName, SDType, rawData);
    return obj (where SDObject implements the Writable interface

    2. deserialize(Object obj);
    SDObject obj = new SDObject(tableOrColumnName, SDType, rawData);
    return obj;

    // how does one implement RosettaField.get(RosettaObject obj) ?
    //


    3. getFields: call RosettaType.getFields
    4. getFieldFromExpression: call RosettaType.getFields
    5. toJSONString - make serialize type a format.

    // Are we going to allow people to define these types and then re-use them?

    // Another question - does every type have a SerDe??

    }


    */

    if(_columns.containsKey(fieldExpression)) {
      // for now ignore parentField for MetaDataTyped fields since no subtypes
      try {
        return new MetadataTypedSerDeField(fieldExpression, Class.forName("java.lang.String"),_columns.get(fieldExpression));
      } catch(Exception e) {
        throw new SerDeException(e);
      }
    } else if(ExpressionUtils.isComplexExpression(fieldExpression)) {
      // for backwards compatability...
      return new ComplexSerDeField(parentField, fieldExpression, this);
    } else {
      // again for backwards compatability - the ComplexSerDeField calls this to get the type of the "col" in columnset
      String className = type.getName();
      return new ReflectionSerDeField(className, fieldExpression);
    }
  }


  public List<SerDeField> getFields(SerDeField parentField) throws SerDeException {
    // ignore parent for now - famous last words :)

    ArrayList<SerDeField> fields = new ArrayList<SerDeField> (_columns_list.length);

    for(String column: _columns_list) {
      fields.add(this.getFieldFromExpression(null, column));
    }
    return fields;
  }

  public String toJSONString(Object obj, SerDeField hf) throws SerDeException {
    try {
      if(obj == null) {
        return "";
      }
      if (hf == null) {
        // if this is a top level Thrift object
        return json_serializer.toString((TBase)obj);
      }
      if(hf.isList() || hf.isMap()) {
        // pretty print a list
        return SerDeUtils.toJSONString(obj, hf, this);
      }
      if(hf.isPrimitive()) {
        // escape string before printing
        return SerDeUtils.lightEscapeString(obj.toString());
      }

      // anything else must be a top level thrift object as well
      return json_serializer.toString((TBase)obj);
    } catch (TException e) {
      throw new SerDeException("toJSONString:TJSONProtocol error", e);
    }
  }
}
