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

package org.apache.hadoop.hive.serde;

import java.lang.reflect.*;
import java.util.HashMap;

/**
 * The default implementation of Hive Field based on Java Reflection.
 */

public class ReflectionSerDeField implements SerDeField {

  protected Class _parentClass;
  protected Class _class;
  protected Field _field;
  protected boolean _isList;
  protected boolean _isMap;
  protected boolean _isClassPrimitive;
  protected Class _valueClass;
  protected Class _keyClass;

  private static HashMap<String, Field[]> cacheFields = new HashMap<String, Field[]>();

  public static boolean isClassPrimitive(Class c) {
    return ((c == String.class) || (c == Boolean.class) ||
            (c == Character.class) ||
            java.lang.Number.class.isAssignableFrom(c) ||
            c.isPrimitive());
  }

  public ReflectionSerDeField(String className, String fieldName) throws SerDeException {
    try {
      _parentClass = Class.forName(className);
      
      // hack for now. Get all the fields and do a case-insensitive search over them
      //      _field = _parentClass.getDeclaredField(fieldName);
      Field[] allFields = cacheFields.get(className);
      if (allFields == null) {
        allFields = _parentClass.getDeclaredFields();
        cacheFields.put(className, allFields);
      }

      boolean found = false;
      for (Field f: allFields) {
        if (f.getName().equalsIgnoreCase(fieldName)) {
          _field = f;
          found = true;
          break;
        }
      }

      if (!found) 
        throw new SerDeException("Illegal class or member:"+className+"."+fieldName);

      _isList = java.util.List.class.isAssignableFrom(_field.getType());
      _isMap = java.util.Map.class.isAssignableFrom(_field.getType());
      _class = _field.getType();

      if(_isList || _isMap) {
        ParameterizedType ptype =
          (ParameterizedType)_field.getGenericType();
        Type[] targs = ptype.getActualTypeArguments();
        if(_isList) {
          _valueClass = ((Class)targs[0]);
        } else {
          _keyClass = ((Class)targs[0]);
          _valueClass = ((Class)targs[1]);
        }
        _isClassPrimitive = false;
      } else {
        _isClassPrimitive = isClassPrimitive(_class);
      }

    } catch (Exception e) {
      throw new SerDeException("Illegal class or member:"+className+"."+fieldName + ":" + e.getMessage(), e);
    }
  }

  public Object get(Object obj) throws SerDeException {
    try {
      return (_field.get(obj));
    } catch (Exception e) {
      throw new SerDeException("Illegal object or access error", e);
    }
  }

  public boolean isList() {
    return _isList;
  }

  public boolean isMap() {
    return _isMap;
  }

  public boolean isPrimitive() {
    if(_isList || _isMap)
      return false;

    return _isClassPrimitive;
  }

  public Class getType() {
    return _class;
  }

  public Class getListElementType() {
    if(_isList) {
      return _valueClass;
    } else {
      throw new RuntimeException("Not a list field ");
    }
  }

  public Class getMapKeyType() {
    if(_isMap) {
      return _keyClass;
    } else {
      throw new RuntimeException("Not a map field ");
    }
  }

  public Class getMapValueType() {
    if(_isMap) {
      return _valueClass;
    } else {
      throw new RuntimeException("Not a map field ");
    }
  }

  public String getName() {
    return _field.getName().toLowerCase();
  }


  public String toString() {
    return fieldToString(this);
  }

  public static String fieldToString(SerDeField hf) {
    return("Field="+hf.getName() +
           ", isPrimitive="+hf.isPrimitive()+
           ", isList="+hf.isList()+(hf.isList()?" of "+hf.getListElementType().getName():"")+
           ", isMap="+hf.isMap()+(hf.isMap()?" of <"+hf.getMapKeyType().getName()+","
                                  +hf.getMapValueType().getName()+">":"")+
           ", type="+hf.getType().getName());
  }
}
