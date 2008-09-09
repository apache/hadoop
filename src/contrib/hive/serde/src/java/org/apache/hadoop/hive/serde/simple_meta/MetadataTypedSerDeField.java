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
import java.lang.reflect.*;

/**
 * The default implementation of Hive Field based on type info from the metastore
 */


public class MetadataTypedSerDeField implements SerDeField {

  protected String _fieldName;
  protected int _position;

  public String toString() {
    return "[fieldName=" + _fieldName + ",position=" + _position + "]";
  }

  public MetadataTypedSerDeField() throws SerDeException {
    throw new SerDeException("MetaDataTypedSerDeField::empty constructor not implemented!");
  }


  public MetadataTypedSerDeField(String fieldName, Class myClass, int position) throws SerDeException {
    // for now just support String so don't bother with the Class
    _fieldName = fieldName;
    _position = position;
  }

  public Object get(Object obj) throws SerDeException {
    /**
       if we had the runtime thrift, we'd call it to get this field's value:

       ret = runtime_thrift.get(this.serde.ddl, _fieldName, obj);
       // this assumes we have the fully qualified fieldName here (which we should)
       // and that runtime_thrift can understand that.

       // It would suck to have to parse the obj everytime so we should have it
       // somehow pre-parsed and cached somewhere as an opaque object we pass into the
       // runtime thrift.

       pw 2/5/08

    */

    ColumnSet temp = (ColumnSet)obj;
    if(temp.col.size() <= _position) {
      //System.err.println("get " + temp.col.size() + "<=" + _position);
      return null;
    }
    try {
      final String ret =  temp.col.get(_position);
      return ret;
    } catch (Exception e) {
      System.err.println("ERR: MetaDataTypedSerDeField::get cannot access field - " +
                         "fieldName=" + _fieldName + ",obj=" + obj + ",length=" + temp.col.size());
      throw new SerDeException("Illegal object or access error: " + e.getMessage());
    }
  }

  public boolean isList() {
    return false;
  }

  public boolean isMap() {
    return false;
  }

  public boolean isPrimitive() {
    return true;
  }

  public Class getType() {
    // total hack. Since fieldName is a String, this does the right thing :) pw 2/5/08
    return this._fieldName.getClass();
  }

  public Class getListElementType() {
    throw new RuntimeException("Not a list field ");
  }

  public Class getMapKeyType() {
    throw new RuntimeException("Not a map field ");
  }

  public Class getMapValueType() {
    throw new RuntimeException("Not a map field ");
  }

  public String getName() {
    return _fieldName;
  }

  public static String fieldToString(SerDeField hf) {
    return("Field= "+hf.getName() + ":String");
  }
}
