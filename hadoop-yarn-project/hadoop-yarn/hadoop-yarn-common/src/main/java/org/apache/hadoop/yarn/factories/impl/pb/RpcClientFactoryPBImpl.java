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

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;

@Private
public class RpcClientFactoryPBImpl implements RpcClientFactory {

  private static final Logger LOG = LoggerFactory
      .getLogger(RpcClientFactoryPBImpl.class);

  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.client";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBClientImpl";
  
  private static final RpcClientFactoryPBImpl self = new RpcClientFactoryPBImpl();
  private ConcurrentMap<Class<?>, Constructor<?>> cache = new ConcurrentHashMap<Class<?>, Constructor<?>>();
  
  public static RpcClientFactoryPBImpl get() {
    return RpcClientFactoryPBImpl.self;
  }
  
  private RpcClientFactoryPBImpl() {
  }
  
  public Object getClient(Class<?> protocol, long clientVersion,
      InetSocketAddress addr, Configuration conf) {
   
    Constructor<?> constructor = cache.get(protocol);
    if (constructor == null) {
      Class<?> pbClazz = null;
      try {
        pbClazz = conf.getClassByName(getPBImplClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException("Failed to load class: ["
            + getPBImplClassName(protocol) + "]", e);
      }
      try {
        constructor = pbClazz.getConstructor(Long.TYPE, InetSocketAddress.class, Configuration.class);
        constructor.setAccessible(true);
        cache.putIfAbsent(protocol, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnRuntimeException("Could not find constructor with params: " + Long.TYPE + ", " + InetSocketAddress.class + ", " + Configuration.class, e);
      }
    }
    try {
      Object retObject = constructor.newInstance(clientVersion, addr, conf);
      return retObject;
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    } catch (InstantiationException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public void stopClient(Object proxy) {
    try {
      if (proxy instanceof Closeable) {
        ((Closeable) proxy).close();
        return;
      } else {
        InvocationHandler handler = Proxy.getInvocationHandler(proxy);
        if (handler instanceof Closeable) {
          ((Closeable) handler).close();
          return;
        }
      }
    } catch (Exception e) {
      LOG.error("Cannot call close method due to Exception. " + "Ignoring.", e);
      throw new YarnRuntimeException(e);
    }
    throw new HadoopIllegalArgumentException(
      "Cannot close proxy - is not Closeable or "
          + "does not provide closeable invocation handler " + proxy.getClass());
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