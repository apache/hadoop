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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector instances.
 * 
 * SerDe classes should call the static functions in this library to create an ObjectInspector
 * to return to the caller of SerDe2.getObjectInspector(). 
 */
public class ObjectInspectorUtils {

  private static Log LOG = LogFactory.getLog(ObjectInspectorUtils.class.getName());
  
  /** This function defines the list of PrimitiveClasses that we support. 
   *  A PrimitiveClass should support java serialization/deserialization.
   */
  public static boolean isPrimitiveClass(Class<?> c) {
    return ((c == String.class) || (c == Boolean.class) ||
            (c == Character.class) || (c == java.sql.Date.class) || 
            java.lang.Number.class.isAssignableFrom(c) ||
            c.isPrimitive());
  }
  
  /**
   * Generalize the Java primitive types to the corresponding 
   * Java Classes.  
   */
  public static Class<?> generalizePrimitive(Class<?> primitiveClass) {
    if (primitiveClass == Boolean.TYPE)   primitiveClass = Boolean.class;
    if (primitiveClass == Byte.TYPE)      primitiveClass = Byte.class;
    if (primitiveClass == Character.TYPE) primitiveClass = Character.class;
    if (primitiveClass == Short.TYPE)     primitiveClass = Short.class;
    if (primitiveClass == Integer.TYPE)   primitiveClass = Integer.class;
    if (primitiveClass == Long.TYPE)      primitiveClass = Long.class;
    if (primitiveClass == Float.TYPE)     primitiveClass = Float.class;
    if (primitiveClass == Double.TYPE)    primitiveClass = Double.class;
    if (primitiveClass == Void.TYPE)      primitiveClass = Void.class;
    return primitiveClass;
  }
  
  /**
   * Get the short name for the types
   */
  public static String getClassShortName(String className) {
    String result = className;
    
    if (result.equals(String.class.getName())) {
      result = org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME;
    } else if (result.equals(Integer.class.getName())) {
      result = org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME;
    } else if (result.equals(Float.class.getName())) {
      result = org.apache.hadoop.hive.serde.Constants.FLOAT_TYPE_NAME;
    } else if (result.equals(Double.class.getName())) {
      result = org.apache.hadoop.hive.serde.Constants.DOUBLE_TYPE_NAME;
    } else if (result.equals(Long.class.getName())) {
      result = org.apache.hadoop.hive.serde.Constants.BIGINT_TYPE_NAME;
    } else if (result.equals(java.sql.Date.class.getName())) {
      result = org.apache.hadoop.hive.serde.Constants.DATE_TYPE_NAME;
    } else {
      LOG.warn("unsupported class: " + className);
    }
    
    // Remove prefix
    String prefix = "java.lang.";
    if (result.startsWith(prefix)) {
      result = result.substring(prefix.length());
    }
    
    return result;
  }
  
  static ArrayList<ArrayList<String>> integerArrayCache = new ArrayList<ArrayList<String>>();
  /**
   * Returns an array of Integer strings, starting from "0".
   * This function caches the arrays to provide a better performance. 
   */
  public static ArrayList<String> getIntegerArray(int size) {
    while (integerArrayCache.size() <= size) {
      integerArrayCache.add(null);
    }
    ArrayList<String> result = integerArrayCache.get(size);
    if (result == null) {
      result = new ArrayList<String>();
      for (int i=0; i<size; i++) {
        result.add(Integer.valueOf(i).toString());
      }
      integerArrayCache.set(size, result);
    }
    return result;
  }

  static ArrayList<String> integerCSVCache = new ArrayList<String>(); 
  public static String getIntegerCSV(int size) {
    while (integerCSVCache.size() <= size) {
      integerCSVCache.add(null);
    }
    String result = integerCSVCache.get(size);
    if (result == null) {
      StringBuilder sb = new StringBuilder();
      for(int i=0; i<size; i++) {
        if (i>0) sb.append(",");
        sb.append("" + i);
      }
      result = sb.toString();
      integerCSVCache.set(size, result);
    }
    return result;
  }
  

  /**
   * Get the standard ObjectInspector for an ObjectInspector.
   * 
   * The returned ObjectInspector can be used to inspect the standard object.
   */
  public static ObjectInspector getStandardObjectInspector(ObjectInspector oi) {
    ObjectInspector result = null;
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi =(PrimitiveObjectInspector)oi;
        result = poi;
        break;
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector)oi;
        result = ObjectInspectorFactory.getStandardListObjectInspector(loi.getListElementObjectInspector());
        break;
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector)oi;
        result = ObjectInspectorFactory.getStandardMapObjectInspector(
            moi.getMapKeyObjectInspector(),
            moi.getMapValueObjectInspector());
        break;
      }
      case STRUCT: {
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<String> fieldNames = new ArrayList<String>(fields.size());
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fields.size());
        for(StructField f : fields) {
          fieldNames.add(f.getFieldName());
          fieldObjectInspectors.add(f.getFieldObjectInspector());
        }
        result = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
        break;
      }
      default: {
        throw new RuntimeException("Unknown ObjectInspector category!");
      }
    }
    return result;
  }
  
  // TODO: should return o if the ObjectInspector is a standard ObjectInspector hierarchy
  // (all internal ObjectInspector needs to be standard ObjectInspectors)
  public static Object getStandardObject(Object o, ObjectInspector oi) {
    if (o == null) {
      return null;
    }
    
    Object result = null;
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        result = o;
        break;
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector)oi;
        int length = loi.getListLength(o);
        ArrayList<Object> list = new ArrayList<Object>(length);
        for(int i=0; i<length; i++) {
          list.add(getStandardObject(
              loi.getListElement(o, i),
              loi.getListElementObjectInspector()));
        }
        result = list;
        break;
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector)oi;
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        Map<? extends Object, ? extends Object> omap = moi.getMap(o);
        for(Map.Entry<? extends Object, ? extends Object> entry: omap.entrySet()) {
          map.put(getStandardObject(entry.getKey(), moi.getMapKeyObjectInspector()),
              getStandardObject(entry.getValue(), moi.getMapValueObjectInspector()));
        }
        result = map;
        break;
      }
      case STRUCT: {
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        ArrayList<Object> struct = new ArrayList<Object>(fields.size()); 
        for(StructField f : fields) {
          struct.add(getStandardObject(soi.getStructFieldData(o, f), f.getFieldObjectInspector()));
        }
        result = struct;
        break;
      }
      default: {
        throw new RuntimeException("Unknown ObjectInspector category!");
      }
    }
    return result;
  }  
  
  public static String getStandardStructTypeName(StructObjectInspector soi) {
    StringBuilder sb = new StringBuilder();
    sb.append("struct{");
    List<? extends StructField> fields = soi.getAllStructFieldRefs(); 
    for(int i=0; i<fields.size(); i++) {
      if (i>0) sb.append(",");
      sb.append(fields.get(i).getFieldName());
      sb.append(":");
      sb.append(fields.get(i).getFieldObjectInspector().getTypeName());
    }
    sb.append("}");
    return sb.toString();
  }
  
  public static StructField getStandardStructFieldRef(String fieldName, List<? extends StructField> fields) {
    fieldName = fieldName.toLowerCase();
    for(int i=0; i<fields.size(); i++) {
      if (fields.get(i).getFieldName().equals(fieldName)) {
        return fields.get(i);
      }
    }
    throw new RuntimeException("cannot find field " + fieldName + " from " + fields); 
    // return null;
  }

  
  
}
