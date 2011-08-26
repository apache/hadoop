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
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;

public class RpcClientFactoryPBImpl implements RpcClientFactory {

  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.client";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBClientImpl";
  
  private static final RpcClientFactoryPBImpl self = new RpcClientFactoryPBImpl();
  private Configuration localConf = new Configuration();
  private ConcurrentMap<Class<?>, Constructor<?>> cache = new ConcurrentHashMap<Class<?>, Constructor<?>>();
  
  public static RpcClientFactoryPBImpl get() {
    return RpcClientFactoryPBImpl.self;
  }
  
  private RpcClientFactoryPBImpl() {
  }
  
  public Object getClient(Class<?> protocol, long clientVersion, InetSocketAddress addr, Configuration conf) throws YarnException {
   
    Constructor<?> constructor = cache.get(protocol);
    if (constructor == null) {
      Class<?> pbClazz = null;
      try {
        pbClazz = localConf.getClassByName(getPBImplClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnException("Failed to load class: ["
            + getPBImplClassName(protocol) + "]", e);
      }
      try {
        constructor = pbClazz.getConstructor(Long.TYPE, InetSocketAddress.class, Configuration.class);
        constructor.setAccessible(true);
        cache.putIfAbsent(protocol, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnException("Could not find constructor with params: " + Long.TYPE + ", " + InetSocketAddress.class + ", " + Configuration.class, e);
      }
    }
    try {
      Object retObject = constructor.newInstance(clientVersion, addr, conf);
      return retObject;
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    } catch (InstantiationException e) {
      throw new YarnException(e);
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