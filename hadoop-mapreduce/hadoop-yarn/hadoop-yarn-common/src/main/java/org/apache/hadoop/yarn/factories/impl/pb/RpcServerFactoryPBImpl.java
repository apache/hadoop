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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RpcServerFactory;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;

import com.google.protobuf.BlockingService;

public class RpcServerFactoryPBImpl implements RpcServerFactory {

  private static final String PROTO_GEN_PACKAGE_NAME = "org.apache.hadoop.yarn.proto";
  private static final String PROTO_GEN_CLASS_SUFFIX = "Service";
  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.service";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBServiceImpl";
  
  private static final RpcServerFactoryPBImpl self = new RpcServerFactoryPBImpl();

  private Configuration localConf = new Configuration();
  private ConcurrentMap<Class<?>, Constructor<?>> serviceCache = new ConcurrentHashMap<Class<?>, Constructor<?>>();
  private ConcurrentMap<Class<?>, Method> protoCache = new ConcurrentHashMap<Class<?>, Method>();
  
  public static RpcServerFactoryPBImpl get() {
    return RpcServerFactoryPBImpl.self;
  }
  

  private RpcServerFactoryPBImpl() {
  }
  
  @Override
  public Server getServer(Class<?> protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager, int numHandlers)
      throws YarnException {
    
    Constructor<?> constructor = serviceCache.get(protocol);
    if (constructor == null) {
      Class<?> pbServiceImplClazz = null;
      try {
        pbServiceImplClazz = localConf
            .getClassByName(getPbServiceImplClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnException("Failed to load class: ["
            + getPbServiceImplClassName(protocol) + "]", e);
      }
      try {
        constructor = pbServiceImplClazz.getConstructor(protocol);
        constructor.setAccessible(true);
        serviceCache.putIfAbsent(protocol, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnException("Could not find constructor with params: "
            + Long.TYPE + ", " + InetSocketAddress.class + ", "
            + Configuration.class, e);
      }
    }
    
    Object service = null;
    try {
      service = constructor.newInstance(instance);
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    } catch (InstantiationException e) {
      throw new YarnException(e);
    }

    Method method = protoCache.get(protocol);
    if (method == null) {
      Class<?> protoClazz = null;
      try {
        protoClazz = localConf.getClassByName(getProtoClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnException("Failed to load class: ["
            + getProtoClassName(protocol) + "]", e);
      }
      try {
        method = protoClazz.getMethod("newReflectiveBlockingService", service.getClass().getInterfaces()[0]);
        method.setAccessible(true);
        protoCache.putIfAbsent(protocol, method);
      } catch (NoSuchMethodException e) {
        throw new YarnException(e);
      }
    }
    
    try {
      return createServer(addr, conf, secretManager, numHandlers,
          (BlockingService)method.invoke(null, service));
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }
  
  private String getProtoClassName(Class<?> clazz) {
    String srcClassName = getClassName(clazz);
    return PROTO_GEN_PACKAGE_NAME + "." + srcClassName + "$" + srcClassName + PROTO_GEN_CLASS_SUFFIX;  
  }
  
  private String getPbServiceImplClassName(Class<?> clazz) {
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

  private Server createServer(InetSocketAddress addr, Configuration conf, 
      SecretManager<? extends TokenIdentifier> secretManager, int numHandlers, 
      BlockingService blockingService) throws IOException {
    RPC.setProtocolEngine(conf, BlockingService.class, ProtoOverHadoopRpcEngine.class);
    Server server = RPC.getServer(BlockingService.class, blockingService, 
        addr.getHostName(), addr.getPort(), numHandlers, false, conf, 
        secretManager);
    return server;
  }
}
