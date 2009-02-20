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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * TypeInfoFactory can be used to create the TypeInfo object for any types.
 * 
 * TypeInfo objects are all read-only so we can reuse them easily. TypeInfoFactory 
 * has internal cache to make sure we don't create 2 TypeInfo objects that represents the
 * same type.
 */
public class TypeInfoFactory {

  static HashMap<Class<?>, TypeInfo> cachedPrimitiveTypeInfo = new HashMap<Class<?>, TypeInfo>();
  public static TypeInfo getPrimitiveTypeInfo(Class<?> primitiveClass) {
    assert(ObjectInspectorUtils.isPrimitiveClass(primitiveClass));
    primitiveClass = ObjectInspectorUtils.generalizePrimitive(primitiveClass);
    TypeInfo result = cachedPrimitiveTypeInfo.get(primitiveClass);
    if (result == null) { 
      result = new PrimitiveTypeInfo(primitiveClass);
      cachedPrimitiveTypeInfo.put(primitiveClass, result);
    }
    return result;
  }
  
  static HashMap<ArrayList<List<?>>, TypeInfo> cachedStructTypeInfo = new HashMap<ArrayList<List<?>>, TypeInfo>();
  public static TypeInfo getStructTypeInfo(List<String> names, List<TypeInfo> typeInfos) {
    ArrayList<List<?>> signature = new ArrayList<List<?>>(2);
    signature.add(names);
    signature.add(typeInfos);
    TypeInfo result = cachedStructTypeInfo.get(signature);
    if (result == null) { 
      result = new StructTypeInfo(names, typeInfos);
      cachedStructTypeInfo.put(signature, result);
    }
    return result;
  }
  
  static HashMap<TypeInfo, TypeInfo> cachedListTypeInfo = new HashMap<TypeInfo, TypeInfo>();
  public static TypeInfo getListTypeInfo(TypeInfo elementTypeInfo) {
    TypeInfo result = cachedListTypeInfo.get(elementTypeInfo);
    if (result == null) { 
      result = new ListTypeInfo(elementTypeInfo);
      cachedListTypeInfo.put(elementTypeInfo, result);
    }
    return result;
  }

  static HashMap<ArrayList<TypeInfo>, TypeInfo> cachedMapTypeInfo = new HashMap<ArrayList<TypeInfo>, TypeInfo>();
  public static TypeInfo getMapTypeInfo(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo) {
    ArrayList<TypeInfo> signature = new ArrayList<TypeInfo>(2);
    signature.add(keyTypeInfo);
    signature.add(valueTypeInfo);
    TypeInfo result = cachedMapTypeInfo.get(signature);
    if (result == null) { 
      result = new MapTypeInfo(keyTypeInfo, valueTypeInfo);
      cachedMapTypeInfo.put(signature, result);
    }
    return result;
  }
  
  
}
