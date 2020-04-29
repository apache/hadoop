
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

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.lang.reflect.Method;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

/**
 * ContextFactory to reuse JAXBContextImpl for DAO Classes.
 */
public final class ContextFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContextFactory.class);

  private static JAXBContext cacheContext;

  // All the dao classes from TimelineWebService and AHSWebService
  // added except TimelineEntity and TimelineEntities
  private static final Class[] CTYPES = {AppInfo.class, AppsInfo.class,
      AppAttemptInfo.class, AppAttemptsInfo.class, ContainerInfo.class,
      ContainersInfo.class, RemoteExceptionData.class, TimelineDomain.class,
      TimelineDomains.class, TimelineEvents.class, TimelinePutResponse.class};
  private static final Set<Class> CLASS_SET =
      new HashSet<>(Arrays.asList(CTYPES));

  // TimelineEntity has java.util.Set interface which JAXB
  // can't handle and throws IllegalAnnotationExceptions
  private static final Class[] IGNORE_TYPES = {TimelineEntity.class,
      TimelineEntities.class};
  private static final Set<Class> IGNORE_SET =
      new HashSet<>(Arrays.asList(IGNORE_TYPES));

  private static JAXBException je =
      new JAXBException("TimelineEntity and TimelineEntities has " +
      "IllegalAnnotation");

  private static StackTraceElement[] stackTrace = new StackTraceElement[]{
      new StackTraceElement(ContextFactory.class.getName(),
      "createContext", "ContextFactory.java", -1)};

  private ContextFactory() {
  }

  public static JAXBContext newContext(Class[] classes,
      Map<String, Object> properties) throws Exception {
    Class spFactory = Class.forName(
        "com.sun.xml.bind.v2.ContextFactory");
    Method m = spFactory.getMethod("createContext", Class[].class, Map.class);
    return (JAXBContext) m.invoke((Object) null, classes, properties);
  }

  // Called from WebComponent.service
  public static JAXBContext createContext(Class[] classes,
      Map<String, Object> properties) throws Exception {
    for (Class c : classes) {
      if (IGNORE_SET.contains(c)) {
        je.setStackTrace(stackTrace);
        throw je;
      }
      if (!CLASS_SET.contains(c)) {
        try {
          return newContext(classes, properties);
        } catch (Exception e) {
          LOG.warn("Error while Creating JAXBContext", e);
          throw e;
        }
      }
    }

    try {
      synchronized (ContextFactory.class) {
        if (cacheContext == null) {
          cacheContext = newContext(CTYPES, properties);
        }
      }
    } catch(Exception e) {
      LOG.warn("Error while Creating JAXBContext", e);
      throw e;
    }
    return cacheContext;
  }

  // Called from WebComponent.init
  public static JAXBContext createContext(String contextPath, ClassLoader
      classLoader, Map<String, Object> properties) throws Exception {
    Class spFactory = Class.forName(
        "com.sun.xml.bind.v2.ContextFactory");
    Method m = spFactory.getMethod("createContext", String.class,
        ClassLoader.class, Map.class);
    return (JAXBContext) m.invoke(null, contextPath, classLoader,
        properties);
  }

}
