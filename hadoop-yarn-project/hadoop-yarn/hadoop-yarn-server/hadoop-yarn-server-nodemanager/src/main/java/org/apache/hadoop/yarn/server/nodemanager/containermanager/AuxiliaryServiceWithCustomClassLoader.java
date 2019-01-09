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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;

final class AuxiliaryServiceWithCustomClassLoader extends AuxiliaryService {

  private final AuxiliaryService wrapped;
  private final ClassLoader customClassLoader;

  private AuxiliaryServiceWithCustomClassLoader(String name,
      AuxiliaryService wrapped, ClassLoader customClassLoader) {
    super(name);
    this.wrapped = wrapped;
    this.customClassLoader = customClassLoader;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // We pass a shared configuration as part of serviceInit call.
    // To avoid the scenario that we could get a ClassNotFoundException
    // when we use customClassLoader to load the class, we create a copy
    // of the configuration.
    Configuration config = new Configuration(conf);
    // reset the service configuration
    setConfig(config);
    config.setClassLoader(customClassLoader);
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.init(config);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.start();
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.stop();
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void initializeApplication(
      ApplicationInitializationContext initAppContext) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.initializeApplication(initAppContext);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext stopAppContext) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.stopApplication(stopAppContext);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public ByteBuffer getMetaData() {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      return wrapped.getMetaData();
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void initializeContainer(ContainerInitializationContext
      initContainerContext) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.initializeContainer(initContainerContext);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void stopContainer(ContainerTerminationContext stopContainerContext) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.stopContainer(stopContainerContext);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void setRecoveryPath(Path recoveryPath) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      wrapped.setRecoveryPath(recoveryPath);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  public static AuxiliaryServiceWithCustomClassLoader getInstance(
      Configuration conf, String className, String appClassPath, String[]
      systemClasses) throws IOException, ClassNotFoundException {
    ClassLoader customClassLoader = createAuxServiceClassLoader(
        appClassPath, systemClasses);
    Class<?> clazz = Class.forName(className, true,
        customClassLoader);
    Class<? extends AuxiliaryService> sClass = clazz.asSubclass(
        AuxiliaryService.class);
    AuxiliaryService wrapped = ReflectionUtils.newInstance(sClass, conf);
    return new AuxiliaryServiceWithCustomClassLoader(
        className + " with custom class loader", wrapped,
        customClassLoader);
  }

  private static ClassLoader createAuxServiceClassLoader(
      final String appClasspath, final String[] systemClasses)
      throws IOException {
    try {
      return AccessController.doPrivileged(
        new PrivilegedExceptionAction<ClassLoader>() {
          @Override
          public ClassLoader run() throws MalformedURLException {
            return new ApplicationClassLoader(appClasspath,
                AuxServices.class.getClassLoader(),
                Arrays.asList(systemClasses));
          }
        }
      );
    } catch (PrivilegedActionException e) {
      Throwable t = e.getCause();
      if (t instanceof MalformedURLException) {
        throw (MalformedURLException) t;
      }
      throw new IOException(e);
    }
  }
}