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
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector instances.
 * 
 * SerDe classes should call the static functions in this library to create an ObjectInspector
 * to return to the caller of SerDe2.getObjectInspector(). 
 */
public class ObjectInspectorFactory {


  /**
   * ObjectInspectorOptions describes what ObjectInspector to use. 
   * JAVA is to use pure JAVA reflection. THRIFT is to use JAVA reflection and filter out __isset fields.
   * New ObjectInspectorOptions can be added here when available.
   * 
   * We choose to use a single HashMap objectInspectorCache to cache all situations for efficiency and code 
   * simplicity.  And we don't expect a case that a user need to create 2 or more different types of 
   * ObjectInspectors for the same Java type.
   */
  public enum ObjectInspectorOptions {
    JAVA,
    THRIFT
  };
  
  private static HashMap<Type, ObjectInspector> objectInspectorCache = new HashMap<Type, ObjectInspector>();
  
  public static ObjectInspector getReflectionObjectInspector(Type t, ObjectInspectorOptions options) {
    ObjectInspector oi = objectInspectorCache.get(t);
    if (oi == null) {
      oi = getReflectionObjectInspectorNoCache(t, options);
      objectInspectorCache.put(t, oi);
    }
    if ((options.equals(ObjectInspectorOptions.JAVA) && oi.getClass().equals(ThriftStructObjectInspector.class))
        || (options.equals(ObjectInspectorOptions.THRIFT) && oi.getClass().equals(ReflectionStructObjectInspector.class))) {
      throw new RuntimeException("Cannot call getObjectInspectorByReflection with both JAVA and THRIFT !");
    }
    return oi;
  }
  
  private static ObjectInspector getReflectionObjectInspectorNoCache(Type t, ObjectInspectorOptions options) {
    if (t instanceof GenericArrayType) {
      GenericArrayType at = (GenericArrayType)t;
      return getStandardListObjectInspector(
          getReflectionObjectInspector(at.getGenericComponentType(), options));
    }

    if (t instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType)t;
      // List?
      if (List.class.isAssignableFrom((Class<?>)pt.getRawType())) {
        return getStandardListObjectInspector(
            getReflectionObjectInspector(pt.getActualTypeArguments()[0], options));
      }
      // Map?
      if (Map.class.isAssignableFrom((Class<?>)pt.getRawType())) {
        return getStandardMapObjectInspector(
            getReflectionObjectInspector(pt.getActualTypeArguments()[0], options),
            getReflectionObjectInspector(pt.getActualTypeArguments()[1], options));
      }
      // Otherwise convert t to RawType so we will fall into the following if block.
      t = pt.getRawType();
    }
    
    // Must be a class.
    if (!(t instanceof Class)) {
      throw new RuntimeException(ObjectInspectorFactory.class.getName() + ": internal error."); 
    }
    Class<?> c = (Class<?>)t;
    
    // Primitive?
    if (ObjectInspectorUtils.isPrimitiveClass(c)) {
      return getStandardPrimitiveObjectInspector(c);
    }
    
    // Must be struct because List and Map need to be ParameterizedType
    assert(!List.class.isAssignableFrom(c));
    assert(!Map.class.isAssignableFrom(c));
    
    // Create StructObjectInspector
    ReflectionStructObjectInspector oi;
    switch(options) {
    case JAVA: 
      oi = new ReflectionStructObjectInspector();
      break;
    case THRIFT: 
      oi = new ThriftStructObjectInspector();
      break;
    default:
      throw new RuntimeException(ObjectInspectorFactory.class.getName() + ": internal error."); 
    }
    // put it into the cache BEFORE it is initialized to make sure we can catch recursive types. 
    objectInspectorCache.put(t, oi);
    Field[] fields = c.getDeclaredFields();
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(fields.length);
    for(int i=0; i<fields.length; i++) {
      if (!oi.shouldIgnoreField(fields[i].getName())) {
        structFieldObjectInspectors.add(getReflectionObjectInspector(fields[i].getGenericType(), options));
      }
    }
    oi.init(c, structFieldObjectInspectors);
    return oi;
  }
  
  
  private static HashMap<Class<?>, StandardPrimitiveObjectInspector> cachedStandardPrimitiveInspectorCache = new HashMap<Class<?>, StandardPrimitiveObjectInspector>();
  public static StandardPrimitiveObjectInspector getStandardPrimitiveObjectInspector(Class<?> c) {
    c = ObjectInspectorUtils.generalizePrimitive(c);
    StandardPrimitiveObjectInspector result = cachedStandardPrimitiveInspectorCache.get(c);
    if (result == null) {
      result = new StandardPrimitiveObjectInspector(c);
      cachedStandardPrimitiveInspectorCache.put(c, result);
    }
    return result;
  }
  
  static HashMap<ObjectInspector, StandardListObjectInspector> cachedStandardListObjectInspector =
    new HashMap<ObjectInspector, StandardListObjectInspector>(); 
  public static StandardListObjectInspector getStandardListObjectInspector(ObjectInspector listElementObjectInspector) {
    StandardListObjectInspector result = cachedStandardListObjectInspector.get(listElementObjectInspector);
    if (result == null) {
      result = new StandardListObjectInspector(listElementObjectInspector);
      cachedStandardListObjectInspector.put(listElementObjectInspector, result);
    }
    return result;
  }

  static HashMap<List<ObjectInspector>, StandardMapObjectInspector> cachedStandardMapObjectInspector =
    new HashMap<List<ObjectInspector>, StandardMapObjectInspector>(); 
  public static StandardMapObjectInspector getStandardMapObjectInspector(ObjectInspector mapKeyObjectInspector, ObjectInspector mapValueObjectInspector) {
    ArrayList<ObjectInspector> signature = new ArrayList<ObjectInspector>(2);
    signature.add(mapKeyObjectInspector);
    signature.add(mapValueObjectInspector);
    StandardMapObjectInspector result = cachedStandardMapObjectInspector.get(signature);
    if (result == null) {
      result = new StandardMapObjectInspector(mapKeyObjectInspector, mapValueObjectInspector);
      cachedStandardMapObjectInspector.put(signature, result);
    }
    return result;
  }
  
  static HashMap<ArrayList<List<?>>, StandardStructObjectInspector> cachedStandardStructObjectInspector =
    new HashMap<ArrayList<List<?>>, StandardStructObjectInspector>(); 
  public static StandardStructObjectInspector getStandardStructObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
    ArrayList<List<?>> signature = new ArrayList<List<?>>();
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
    StandardStructObjectInspector result = cachedStandardStructObjectInspector.get(signature);
    if (result == null) {
      result = new StandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
      cachedStandardStructObjectInspector.put(signature, result);
    }
    return result;
  }
  
  static HashMap<List<StructObjectInspector>, UnionStructObjectInspector> cachedUnionStructObjectInspector =
    new HashMap<List<StructObjectInspector>, UnionStructObjectInspector>(); 
  public static UnionStructObjectInspector getUnionStructObjectInspector(List<StructObjectInspector> structObjectInspectors) {
    UnionStructObjectInspector result = cachedUnionStructObjectInspector.get(structObjectInspectors);
    if (result == null) {
      result = new UnionStructObjectInspector(structObjectInspectors);
      cachedUnionStructObjectInspector.put(structObjectInspectors, result);
    }
    return result;
  }
  
  
}
