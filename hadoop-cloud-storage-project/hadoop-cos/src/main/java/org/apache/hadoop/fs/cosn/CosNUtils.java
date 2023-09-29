/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.qcloud.cos.auth.COSCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.auth.COSCredentialsProviderList;
import org.apache.hadoop.fs.cosn.auth.EnvironmentVariableCredentialsProvider;
import org.apache.hadoop.fs.cosn.auth.SimpleCredentialsProvider;

/**
 * Utility methods for CosN code.
 */
public final class CosNUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CosNUtils.class);

  static final String INSTANTIATION_EXCEPTION
      = "instantiation exception";
  static final String NOT_COS_CREDENTIAL_PROVIDER
      = "is not cos credential provider";
  static final String ABSTRACT_CREDENTIAL_PROVIDER
      = "is abstract and therefore cannot be created";

  private CosNUtils() {
  }

  public static COSCredentialsProviderList createCosCredentialsProviderSet(
      URI uri,
      Configuration conf) throws IOException {
    COSCredentialsProviderList credentialProviderList =
        new COSCredentialsProviderList();

    Class<?>[] cosClasses = CosNUtils.loadCosProviderClasses(
        conf,
        CosNConfigKeys.COSN_CREDENTIALS_PROVIDER);
    if (0 == cosClasses.length) {
      credentialProviderList.add(
          new SimpleCredentialsProvider(uri, conf));
      credentialProviderList.add(
          new EnvironmentVariableCredentialsProvider(uri, conf));
    } else {
      for (Class<?> credClass : cosClasses) {
        credentialProviderList.add(createCOSCredentialProvider(uri, conf,
            credClass));
      }
    }

    return credentialProviderList;
  }

  public static Class<?>[] loadCosProviderClasses(
      Configuration conf,
      String key,
      Class<?>... defaultValue) throws IOException {
    try {
      return conf.getClasses(key, defaultValue);
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException("From option " + key + ' ' + c, c);
    }
  }

  public static COSCredentialsProvider createCOSCredentialProvider(
      URI uri,
      Configuration conf,
      Class<?> credClass) throws IOException {
    COSCredentialsProvider credentialsProvider;
    if (!COSCredentialsProvider.class.isAssignableFrom(credClass)) {
      throw new IllegalArgumentException("class " + credClass + " " +
          NOT_COS_CREDENTIAL_PROVIDER);
    }
    if (Modifier.isAbstract(credClass.getModifiers())) {
      throw new IllegalArgumentException("class " + credClass + " " +
          ABSTRACT_CREDENTIAL_PROVIDER);
    }
    LOG.debug("Credential Provider class: " + credClass.getName());

    try {
      // new credClass()
      Constructor constructor = getConstructor(credClass);
      if (constructor != null) {
        credentialsProvider =
            (COSCredentialsProvider) constructor.newInstance();
        return credentialsProvider;
      }
      // new credClass(conf)
      constructor = getConstructor(credClass, Configuration.class);
      if (null != constructor) {
        credentialsProvider =
            (COSCredentialsProvider) constructor.newInstance(conf);
        return credentialsProvider;
      }

      // new credClass(uri, conf)
      constructor = getConstructor(credClass, URI.class,
          Configuration.class);
      if (null != constructor) {
        credentialsProvider =
            (COSCredentialsProvider) constructor.newInstance(uri,
                conf);
        return credentialsProvider;
      }

      Method factory = getFactoryMethod(credClass,
          COSCredentialsProvider.class, "getInstance");
      if (null != factory) {
        credentialsProvider = (COSCredentialsProvider) factory.invoke(null);
        return credentialsProvider;
      }

      throw new IllegalArgumentException(
          "Not supported constructor or factory method found"
      );

    } catch (IllegalAccessException e) {
      throw new IOException(
          credClass.getName() + " " + INSTANTIATION_EXCEPTION + ": " + e, e);
    } catch (InstantiationException e) {
      throw new IOException(
          credClass.getName() + " " + INSTANTIATION_EXCEPTION + ": " + e, e);
    } catch (InvocationTargetException e) {
      Throwable targetException = e.getTargetException();
      if (targetException == null) {
        targetException = e;
      }
      throw new IOException(
          credClass.getName() + " " + INSTANTIATION_EXCEPTION + ": "
              + targetException, targetException);
    }
  }

  private static Constructor<?> getConstructor(Class<?> cl, Class<?>... args) {
    try {
      Constructor constructor = cl.getDeclaredConstructor(args);
      return Modifier.isPublic(constructor.getModifiers()) ? constructor : null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private static Method getFactoryMethod(
      Class<?> cl, Class<?> returnType, String methodName) {
    try {
      Method m = cl.getDeclaredMethod(methodName);
      if (Modifier.isPublic(m.getModifiers())
          && Modifier.isStatic(m.getModifiers())
          && returnType.isAssignableFrom(m.getReturnType())) {
        return m;
      } else {
        return null;
      }
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
