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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ReflectionStructObjectInspector works on struct data that is stored as a native Java object.
 * It will drill down into the Java class to get the fields and construct ObjectInspectors for 
 * the fields, if they are not specified.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class. 
 *
 */
public class ReflectionStructObjectInspector implements StructObjectInspector {

  public static class MyField implements StructField {
    protected Field field;
    protected ObjectInspector fieldObjectInspector;
    
    public MyField(Field field, ObjectInspector fieldObjectInspector) {
      this.field = field;
      this.fieldObjectInspector = fieldObjectInspector;
    }
    public String getFieldName() {
      return field.getName().toLowerCase();
    }
    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }
    public String toString() {
      return field.toString();
    }
  }

  Class<?> objectClass;
  List<MyField> fields;
  
  public ReflectionStructObjectInspector(Class<?> objectClass, List<ObjectInspector> structFieldObjectInspectors) {
    init(objectClass, structFieldObjectInspectors);
  }

  public Category getCategory() {
    return Category.STRUCT;
  }

  public String getTypeName() {
    return objectClass.getName();
  }
  
  /**
   * This method is only intended to be used by the Utilities class in this package.
   * This creates an uninitialized ObjectInspector so the Utilities class can put it into
   * a cache before it initializes when it might look up the cache for member fields that
   * might be of the same type (e.g. recursive type like linked list and trees).
   */
  ReflectionStructObjectInspector() {
  }
  
  /**
   * This method is only intended to be used by Utilities class in this package.
   * The reason that this method is not recursive by itself is because we want to allow
   * recursive types.
   */
  void init(Class<?> objectClass, List<ObjectInspector> structFieldObjectInspectors) {
    assert(!List.class.isAssignableFrom(objectClass));
    assert(!Map.class.isAssignableFrom(objectClass));
    
    this.objectClass = objectClass;
    Field[] reflectionFields = objectClass.getDeclaredFields();
    fields = new ArrayList<MyField>(structFieldObjectInspectors.size());
    int used = 0;
    for (int i=0; i<reflectionFields.length; i++) {
      if (!shouldIgnoreField(reflectionFields[i].getName())) {
        fields.add(new MyField(reflectionFields[i], structFieldObjectInspectors.get(used++)));
      }
    }
    assert(fields.size() == structFieldObjectInspectors.size());
  }
  
  // ThriftStructObjectInspector will override and ignore __isset fields. 
  public boolean shouldIgnoreField(String name) {
    return false;
  }
  
  // Without Data
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  // With Data
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    if (!(fieldRef instanceof MyField)) {
      throw new RuntimeException("fieldRef has to be of MyField");
    }
    MyField f = (MyField) fieldRef;
    try {
      Object r = f.field.get(data);
      return r;
    } catch (Exception e) {
      throw new RuntimeException("cannot get field " + f.field + " from " 
    		  + data.getClass() + " " + data); 
    }
  }
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    try {
      ArrayList<Object> result = new ArrayList<Object>(fields.size());
      for(int i=0; i<fields.size(); i++) {
        result.add(fields.get(i).field.get(data));
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e); 
    }
  }

}
