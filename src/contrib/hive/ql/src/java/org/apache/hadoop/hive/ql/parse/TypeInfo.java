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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.serde.SerDe;
import org.apache.hadoop.hive.serde.SerDeField;

/**
 * Stores information about a type.
 * 
 * This class fixes an important problem in SerDe: SerDeField.getListElementType(), 
 * SerDeField.getMapKeyType(), and SerDeField.getMapValueType() should return a type 
 * description object, instead of the Class object, so we can support types that are 
 * not represented by a java class. 
 * 
 * The current implementation is just a simple wrapper around SerDe and SerDeField.
 * Later it might be possible to merge this class into SerDe etc.
 * 
 * Note: It may not be possible to store the Type information as a Java Class object,
 * because we support all SerDe's including those that are not using Java Reflection. 
 * 
 * 
 * We support 4 categories of types:
 * 1. Primitive objects (String, Number, etc)
 * 2. List objects (a list of objects of a single type)
 * 3. Map objects (a map from objects of one type to objects of another type)
 * 4. Struct objects
 * 
 */
public class TypeInfo {

  static enum Category {
    UNINITIALIZED, PRIMITIVE, LIST, MAP, STRUCT
  };
  
  Category category;
  
  // Only for primitive objects.
  Class<?>    primitiveClass; 
  
  // For all other 3 categories.
  SerDeField currentField;
  SerDe      currentSerDe;
  
  /**
   * Default constructor.
   */
  public TypeInfo() {
    this.category = Category.UNINITIALIZED;
    this.primitiveClass = null;
    this.currentField = null;
    this.currentSerDe = null;    
  }

  /**
   * Default constructor.
   */
  public TypeInfo(Category category) {
    this.category = category;
    this.primitiveClass = null;
    this.currentField = null;
    this.currentSerDe = null;    
  }

  /**
   * Constructor for primitive TypeInfo.
   * @param primitiveType
   */
  public TypeInfo(Class<?> primitiveType) {
    this.category = Category.PRIMITIVE;
    this.primitiveClass = primitiveType; 
  }
  
  /**
   * Constructor for SerDe-based TypeInfo. 
   * currentField can be null, which means the top-level field.
   * 
   * @param currentField   
   * @param currentSerDe
   */
  public TypeInfo(SerDe currentSerDe, SerDeField currentField) {

    this.currentSerDe = currentSerDe;
    this.currentField = currentField;
    
    if (currentField == null) {
      // Top level field
      this.category = Category.STRUCT;
    } else if (currentField.isList()) {
      this.category = Category.LIST;
    } else if (currentField.isMap()) {
      this.category = Category.MAP;
    } else if (currentField.isPrimitive()) {
      this.category = Category.PRIMITIVE;
      this.primitiveClass = generalizePrimitive(currentField.getType());
      this.currentSerDe = null;
      this.currentField = null;
    } else {
      this.category = Category.STRUCT;
    }
    
  }

  /** Only for primitive type. 
   */
  public Class<?> getPrimitiveClass() {
    assert(this.category == Category.PRIMITIVE);
    return this.primitiveClass;
  }

  /** Only for list type. 
   */
  public TypeInfo getListElementType() {
    assert(this.category == Category.LIST);
    try {
      return new TypeInfo(this.currentSerDe, this.currentSerDe.getFieldFromExpression(this.currentField, "[0]"));
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }
  
  /** Only for map type.
   */
  public TypeInfo getMapKeyType() {
    assert(this.category == Category.MAP);
    return new TypeInfo(this.currentField.getMapKeyType());
  }
  
  /** Only for map type.
   */
  public TypeInfo getMapValueType() {
    assert(this.category == Category.MAP);
    return new TypeInfo(this.currentField.getMapValueType());
  }
  
  /** Only for Struct type.
   */
  public TypeInfo getFieldType(String fieldName) {
    assert(this.category == Category.STRUCT);
    try {
      return new TypeInfo(this.currentSerDe, this.currentSerDe.getFieldFromExpression(this.currentField, fieldName));
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }
  
  /** Only for Struct type.
   */
  public List<TypeInfo> getAllFieldsTypes() {
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    try { 
      for(SerDeField field : this.currentSerDe.getFields(this.currentField)) {
        typeInfos.add(new TypeInfo(this.currentSerDe, field));
      }
    } catch (Exception e){
      throw new RuntimeException(e);
    }
    return typeInfos;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other.getClass() != TypeInfo.class) {
      return false;
    }
    TypeInfo another = (TypeInfo)other;
    if (another == null) return false;
    if (this.category != another.category) return false;
    switch(this.category) {
      case PRIMITIVE: 
        return this.getPrimitiveClass().equals(another.getPrimitiveClass());
      case LIST: 
        return this.getListElementType().equals(another.getListElementType());
      case MAP: 
        return this.getMapKeyType().equals(another.getMapKeyType()) 
            && this.getMapValueType().equals(another.getMapValueType());
      case STRUCT:
        return this.getAllFieldsTypes().equals(another.getAllFieldsTypes());
    }
    return false;
  }
  
  public String toString() {
    switch(this.category) {
      case PRIMITIVE: 
        return this.getPrimitiveClass().toString();
      case LIST: 
        return "List<" + this.getListElementType().toString() + ">";
      case MAP: 
        return "Map<" + this.getMapKeyType().toString() + ","
            + this.getMapValueType().toString() + ">";
      case STRUCT:
        return "Struct(" + this.getAllFieldsTypes().toString() + ")";
    }
    return "(Unknown TypeInfo)";    
  }
  
  private static HashMap<Class<?>, TypeInfo> cache;
  static {
    cache = new HashMap<Class<?>, TypeInfo>(); 
  }
  
  public static TypeInfo getPrimitiveTypeInfo(Class<?> primitiveClass) {
    primitiveClass = generalizePrimitive(primitiveClass);
    TypeInfo result = cache.get(primitiveClass);
    if (result == null) {
      result = new TypeInfo(primitiveClass);
      cache.put(primitiveClass, result);
    }
    return result;
  }
  

  public static Class<?> generalizePrimitive(Class<?> primitiveClass) {
    if (primitiveClass == Boolean.TYPE)    primitiveClass = Boolean.class;
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
  
}
