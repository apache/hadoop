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

package org.apache.hadoop.yarn.factories.impl.pb;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;

@Private
public class RecordFactoryPBImpl implements RecordFactory {

  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBImpl";

  private static final RecordFactoryPBImpl self = new RecordFactoryPBImpl();
  private Configuration localConf = new Configuration();
  private ConcurrentMap<Class<?>, Constructor<?>> cache = new ConcurrentHashMap<Class<?>, Constructor<?>>();

  private RecordFactoryPBImpl() {
  }
  
  public static RecordFactory get() {
    return self;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public <T> T newRecordInstance(Class<T> clazz) {
    
    Constructor<?> constructor = cache.get(clazz);
    if (constructor == null) {
      Class<?> pbClazz = null;
      try {
        pbClazz = localConf.getClassByName(getPBImplClassName(clazz));
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException("Failed to load class: ["
            + getPBImplClassName(clazz) + "]", e);
      }
      try {
        constructor = pbClazz.getConstructor();
        constructor.setAccessible(true);
        cache.putIfAbsent(clazz, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnRuntimeException("Could not find 0 argument constructor", e);
      }
    }
    try {
      Object retObject = constructor.newInstance();
      return (T)retObject;
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    } catch (InstantiationException e) {
      throw new YarnRuntimeException(e);
    }
  }

  private String getPBImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }
  
  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }
  
  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }
}
