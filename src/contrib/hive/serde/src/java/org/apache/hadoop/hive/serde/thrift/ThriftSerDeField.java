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

package org.apache.hadoop.hive.serde.thrift;

import org.apache.hadoop.hive.serde.*;
import java.lang.reflect.*;
import java.util.HashMap;

/**
 * Thrift implementation of SerDeField
 * Uses Reflection for the most part. Uses __isset interface exposed by Thrift
 * to determine whether a field is set
 *
 */
public class ThriftSerDeField extends ReflectionSerDeField {

  private Class issetClass;
  private Field issetField;
  private Field fieldIssetField;
  private static HashMap<String, Field[]> cacheFields = new HashMap<String, Field[]>();

  public ThriftSerDeField(String className, String fieldName) throws SerDeException {
    super(className, fieldName);
    try {
      issetClass = Class.forName(className+"$Isset");
      //      fieldIssetField = issetClass.getDeclaredField(fieldName);
      String name = issetClass.getName();
      Field[] allFields = cacheFields.get(name);
      if (allFields == null) {
        allFields = issetClass.getDeclaredFields();
        cacheFields.put(name, allFields);
      }

      boolean found = false;
      for (Field f: allFields) {
        if (f.getName().equalsIgnoreCase(fieldName)) {
          fieldIssetField = f;
          found = true;
          break;
        }
      }

      if (!found) 
        throw new SerDeException("Not a Thrift Class?");

      issetField = _parentClass.getDeclaredField("__isset");
    } catch (Exception e) {
      throw (new SerDeException("Not a Thrift Class?", e));
    }
  }

  public Object get(Object obj) throws SerDeException {
    try {
      if(fieldIssetField.getBoolean(issetField.get(obj))) {
        return _field.get(obj);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new SerDeException("Illegal object or access error", e);
    }
  }

  public String toString() {
    return "ThriftSerDeField::ReflectionSerDeField[" + super.toString() + "]";
  }
}

