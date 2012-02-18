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

package org.apache.hadoop.yarn.factory.providers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RpcClientFactory;
import org.apache.hadoop.yarn.factories.RpcServerFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;

/**
 * A public static get() method must be present in the Client/Server Factory implementation.
 */
public class RpcFactoryProvider {
  private static final Log LOG = LogFactory.getLog(RpcFactoryProvider.class);

  private RpcFactoryProvider() {
    
  }
  
  
  public static RpcServerFactory getServerFactory(Configuration conf) {
    if (conf == null) {
      conf = new Configuration();
    }
    String serverFactoryClassName = conf.get(YarnConfiguration.IPC_SERVER_FACTORY);
    if (serverFactoryClassName == null) {
      if (conf.get(YarnConfiguration.IPC_SERIALIZER_TYPE, YarnConfiguration.DEFAULT_IPC_SERIALIZER_TYPE).equals(YarnConfiguration.DEFAULT_IPC_SERIALIZER_TYPE)) {
        return RpcServerFactoryPBImpl.get();
      } else {
        throw new YarnException("Unknown serializer: [" + conf.get(YarnConfiguration.IPC_SERIALIZER_TYPE) + "]. Use keys: [" + YarnConfiguration.IPC_CLIENT_FACTORY + "][" + YarnConfiguration.IPC_SERVER_FACTORY + "] to specify factories");
      }
    } else {
      return (RpcServerFactory) getFactoryClassInstance(serverFactoryClassName);
    }
  }
  
  public static RpcClientFactory getClientFactory(Configuration conf) {
    String clientFactoryClassName = conf.get(YarnConfiguration.IPC_CLIENT_FACTORY);
    if (clientFactoryClassName == null) {
      if (conf.get(YarnConfiguration.IPC_SERIALIZER_TYPE, YarnConfiguration.DEFAULT_IPC_SERIALIZER_TYPE).equals(YarnConfiguration.DEFAULT_IPC_SERIALIZER_TYPE)) {
        return RpcClientFactoryPBImpl.get();
      } else {
        throw new YarnException("Unknown serializer: [" + conf.get(YarnConfiguration.IPC_SERIALIZER_TYPE) + "]. Use keys: [" + YarnConfiguration.IPC_CLIENT_FACTORY + "][" + YarnConfiguration.IPC_SERVER_FACTORY + "] to specify factories");
      }
    } else {
      return(RpcClientFactory) getFactoryClassInstance(clientFactoryClassName);
    }
  }

  private static Object getFactoryClassInstance(String factoryClassName) {
    try {
      Class clazz = Class.forName(factoryClassName);
      Method method = clazz.getMethod("get", null);
      method.setAccessible(true);
      return method.invoke(null, null);
    } catch (ClassNotFoundException e) {
      throw new YarnException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnException(e);
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    }
  }
  
}
