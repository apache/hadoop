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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;
import org.apache.hadoop.yarn.factories.RpcServerFactory;

/**
 * A public static get() method must be present in the Client/Server Factory implementation.
 */
@InterfaceAudience.LimitedPrivate({ "MapReduce", "YARN" })
public class RpcFactoryProvider {

  private RpcFactoryProvider() {
    
  }
  
  
  public static RpcServerFactory getServerFactory(Configuration conf) {
    if (conf == null) {
      conf = new Configuration();
    }
    String serverFactoryClassName = conf.get(
        YarnConfiguration.IPC_SERVER_FACTORY_CLASS,
        YarnConfiguration.DEFAULT_IPC_SERVER_FACTORY_CLASS);
    return (RpcServerFactory) getFactoryClassInstance(serverFactoryClassName);
  }

  public static RpcClientFactory getClientFactory(Configuration conf) {
    String clientFactoryClassName = conf.get(
        YarnConfiguration.IPC_CLIENT_FACTORY_CLASS,
        YarnConfiguration.DEFAULT_IPC_CLIENT_FACTORY_CLASS);
    return (RpcClientFactory) getFactoryClassInstance(clientFactoryClassName);
  }

  private static Object getFactoryClassInstance(String factoryClassName) {
    try {
      Class<?> clazz = Class.forName(factoryClassName);
      Method method = clazz.getMethod("get", null);
      method.setAccessible(true);
      return method.invoke(null, null);
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnRuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    }
  }
  
}
