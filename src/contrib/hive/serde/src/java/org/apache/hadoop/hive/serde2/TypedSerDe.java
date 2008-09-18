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

package org.apache.hadoop.hive.serde2;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class TypedSerDe implements SerDe {

  protected Type objectType;
  protected Class<?> objectClass;

  public TypedSerDe(Type objectType) throws SerDeException {
    this.objectType = objectType;
    if (objectType instanceof Class) {
      objectClass = (Class<?>)objectType;
    } else if (objectType instanceof ParameterizedType) {
      objectClass = (Class<?>)(((ParameterizedType)objectType).getRawType());
    } else {
      throw new SerDeException("Cannot create TypedSerDe with type " + objectType);
    }
  }

  protected Object deserializeCache;
  public Object deserialize(Writable blob) throws SerDeException {
    if (deserializeCache == null) {
      return ReflectionUtils.newInstance(objectClass, null);
    } else {
      assert(deserializeCache.getClass().equals(objectClass));
      return deserializeCache;
    }
  }

  public ObjectInspector getObjectInspector() throws SerDeException {
    return ObjectInspectorFactory.getReflectionObjectInspector(objectType,
        getObjectInspectorOptions());
  }

  protected ObjectInspectorFactory.ObjectInspectorOptions getObjectInspectorOptions() {
    return ObjectInspectorFactory.ObjectInspectorOptions.JAVA;
  }
  
  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {
    // do nothing
  }

  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new RuntimeException("not supported");
  }

}
