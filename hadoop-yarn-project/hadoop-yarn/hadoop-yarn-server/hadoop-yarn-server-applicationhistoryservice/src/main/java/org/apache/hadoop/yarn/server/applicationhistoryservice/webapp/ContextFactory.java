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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import java.util.Map;
import java.lang.reflect.Method;
import javax.xml.bind.JAXBContext;

/**
 * ContextFactory to reuse JAXBContextImpl for DAO Classes.
 */
public final class ContextFactory {

  private static JAXBContext jaxbContext;

  private ContextFactory() {
  }

  // Called from WebComponent.service
  public static JAXBContext createContext(Class[] classes,
      Map<String, Object> properties) throws Exception {
    synchronized (ContextFactory.class) {
      if (jaxbContext == null) {
        Class spFactory = Class.forName(
            "com.sun.xml.internal.bind.v2.ContextFactory");
        Method m = spFactory.getMethod("createContext", Class[].class,
            Map.class);
        jaxbContext = (JAXBContext) m.invoke((Object) null, classes,
            properties);
      }
    }
    return jaxbContext;
  }

  // Called from WebComponent.init
  public static JAXBContext createContext(String contextPath, ClassLoader
      classLoader, Map<String, Object> properties) throws Exception {
    Class spFactory = Class.forName(
        "com.sun.xml.internal.bind.v2.ContextFactory");
    Method m = spFactory.getMethod("createContext", String.class,
        ClassLoader.class, Map.class);
    return (JAXBContext) m.invoke(null, contextPath, classLoader,
        properties);
  }

}
