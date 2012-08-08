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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;

public class RecordFactoryProvider {
  private static Configuration defaultConf;
  
  static {
    defaultConf = new Configuration();
  }
  
  private RecordFactoryProvider() {
  }
  
  public static RecordFactory getRecordFactory(Configuration conf) {
    if (conf == null) {
      //Assuming the default configuration has the correct factories set.
      //Users can specify a particular factory by providing a configuration.
      conf = defaultConf;
    }
    String recordFactoryClassName = conf.get(YarnConfiguration.IPC_RECORD_FACTORY);
    if (recordFactoryClassName == null) {
      String serializer = conf.get(YarnConfiguration.IPC_SERIALIZER_TYPE, YarnConfiguration.DEFAULT_IPC_SERIALIZER_TYPE);
      if (serializer.equals(YarnConfiguration.DEFAULT_IPC_SERIALIZER_TYPE)) {
        return RecordFactoryPBImpl.get();
      } else {
        throw new YarnException("Unknown serializer: [" + conf.get(YarnConfiguration.IPC_SERIALIZER_TYPE) + "]. Use keys: [" + YarnConfiguration.IPC_RECORD_FACTORY + "] to specify Record factory");
      }
    } else {
      return (RecordFactory) getFactoryClassInstance(recordFactoryClassName);
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
