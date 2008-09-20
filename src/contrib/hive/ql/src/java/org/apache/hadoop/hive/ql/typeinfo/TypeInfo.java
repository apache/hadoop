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

package org.apache.hadoop.hive.ql.typeinfo;

import java.io.Serializable;import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

/**
 * Stores information about a type (DDL).
 * Always use the TypeInfoFactory to create new TypeInfo objects.
 * 
 * We support 4 categories of types:
 * 1. Primitive objects (String, Number, etc)
 * 2. List objects (a list of objects of a single type)
 * 3. Map objects (a map from objects of one type to objects of another type)
 * 4. Struct objects (a list of fields with names and their own types)
 */
public abstract class TypeInfo implements Serializable {

  protected TypeInfo() {}
  
  public String getTypeName() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getCategory()");
  }
  
  public Category getCategory() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getCategory()");
  }
  
  public Class<?> getPrimitiveClass() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getPrimitiveClass()");
  }
  
  public TypeInfo getListElementTypeInfo() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getListElementTypeInfo()");
  }
  
  public TypeInfo getMapKeyTypeInfo() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getMapKeyTypeInfo()");
  }
  public TypeInfo getMapValueTypeInfo() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getMapValueTypeInfo()");
  }
  
  public List<String> getAllStructFieldNames() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getAllStructFieldNames()");
  }
  public List<TypeInfo> getAllStructFieldTypeInfos() {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getAllStructFieldTypeInfos()");
  }
  public TypeInfo getStructFieldTypeInfo(String field) {
    throw new RuntimeException("Unsupported: " + this.getClass() + ".getStructFieldTypeInfo()");
  }
  
  public String toString() {
    return getTypeName();
  }

}
