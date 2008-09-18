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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ListStructObjectInspector works on struct data that is stored as a Java List or Java Array object.
 * Basically, the fields are stored sequentially in the List object.
 * 
 * The names of the struct fields and the internal structure of the struct fields are specified in 
 * the ctor of the StructObjectInspector.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class. 
 */
public class StandardStructObjectInspector implements StructObjectInspector {

  public static final Log LOG = LogFactory.getLog(StandardStructObjectInspector.class.getName());
  
  protected static class MyField implements StructField {
    protected int fieldID;
    protected String fieldName;
    protected ObjectInspector fieldObjectInspector;
    
    public MyField(int fieldID, String fieldName, ObjectInspector fieldObjectInspector) {
      this.fieldID = fieldID;
      this.fieldName = fieldName.toLowerCase();
      this.fieldObjectInspector = fieldObjectInspector;
    }

    public int getFieldID() {
      return fieldID;
    }
    public String getFieldName() {
      return fieldName;
    }
    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }
    
    public String toString() {
      return "" + fieldID + ":" + fieldName;
    }
  }
  
  protected List<MyField> fields;
  
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }
  
  /** Call ObjectInspectorFactory.getStandardListObjectInspector instead.
   */
  protected StandardStructObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
    init(structFieldNames, structFieldObjectInspectors);
  }
  protected void init(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
    assert(structFieldNames.size() == structFieldObjectInspectors.size());
    
    fields = new ArrayList<MyField>(structFieldNames.size()); 
    for(int i=0; i<structFieldNames.size(); i++) {
      fields.add(new MyField(i, structFieldNames.get(i), structFieldObjectInspectors.get(i)));
    }
  }
  
  protected StandardStructObjectInspector(List<StructField> fields) {
    init(fields);
  }
  protected void init(List<StructField> fields) {
    this.fields = new ArrayList<MyField>(fields.size()); 
    for(int i=0; i<fields.size(); i++) {
      this.fields.add(new MyField(i, fields.get(i).getFieldName(), fields.get(i).getFieldObjectInspector()));
    }
  }

  
  public final Category getCategory() {
    return Category.STRUCT;
  }

  // Without Data
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  boolean warned = false;
  // With Data
  @SuppressWarnings("unchecked")
  public Object getStructFieldData(Object data, StructField fieldRef) {
    List<Object> list = getStructFieldsDataAsList(data);
    if (list == null) {
      return null;
    }
    MyField f = (MyField) fieldRef;
    if (fields.size() != list.size() && !warned) {
      // TODO: remove this
      warned = true;
      LOG.warn("Trying to access " + fields.size() + " fields inside a list of " + list.size()
          + " elements: " + list);
      LOG.warn("ignoring similar errors.");
    }
    int fieldID = f.getFieldID();
    assert(fieldID >= 0 && fieldID < fields.size());
    
    return fieldID >= list.size() ? null : list.get(fieldID);
  }
  @SuppressWarnings("unchecked")
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    if (data.getClass().isArray()) {
      data = java.util.Arrays.asList((Object[])data);
    }
    List<Object> list = (List<Object>) data;
    assert(list.size() == fields.size());
    return list;
  }

  
}
