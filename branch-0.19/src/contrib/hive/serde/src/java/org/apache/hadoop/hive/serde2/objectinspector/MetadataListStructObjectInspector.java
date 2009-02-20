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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.serde2.ColumnSet;

/**
 * StructObjectInspector works on struct data that is stored as a Java List or Java Array object.
 * Basically, the fields are stored sequentially in the List object.
 * 
 * The names of the struct fields and the internal structure of the struct fields are specified in 
 * the ctor of the StructObjectInspector.
 * 
 */
public class MetadataListStructObjectInspector extends StandardStructObjectInspector {

  static HashMap<List<String>, MetadataListStructObjectInspector> cached
     = new HashMap<List<String>, MetadataListStructObjectInspector>();
  public static MetadataListStructObjectInspector getInstance(int fields) {
    return getInstance(ObjectInspectorUtils.getIntegerArray(fields));
  }
  public static MetadataListStructObjectInspector getInstance(List<String> columnNames) {
    MetadataListStructObjectInspector result = cached.get(columnNames);
    if (result == null) {
      result = new MetadataListStructObjectInspector(columnNames);
      cached.put(columnNames, result);
    }
    return result;
  }

  static ArrayList<ObjectInspector> getFieldObjectInspectors(int fields) {
    ArrayList<ObjectInspector> r = new ArrayList<ObjectInspector>(fields);
    for(int i=0; i<fields; i++) {
      r.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
    }
    return r;
  }
  
  MetadataListStructObjectInspector(List<String> columnNames) {
    super(columnNames, getFieldObjectInspectors(columnNames.size()));
  }
  
  // Get col object out
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data instanceof ColumnSet) {
      data = ((ColumnSet)data).col;
    }
    return super.getStructFieldData(data, fieldRef);
  }
  // Get col object out
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data instanceof ColumnSet) {
      data = ((ColumnSet)data).col;
    }
    return super.getStructFieldsDataAsList(data);
  }

}
