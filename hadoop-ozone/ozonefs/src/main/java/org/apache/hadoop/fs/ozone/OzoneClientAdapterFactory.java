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
package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.hadoop.fs.StorageStatistics;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates OzoneClientAdapter with classloader separation.
 */
public final class OzoneClientAdapterFactory {

  static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientAdapterFactory.class);

  private OzoneClientAdapterFactory() {
  }

  @SuppressFBWarnings("DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED")
  public static OzoneClientAdapter createAdapter(
      String volumeStr,
      String bucketStr) throws IOException {
    return createAdapter(volumeStr, bucketStr, true,
        (aClass) -> (OzoneClientAdapter) aClass
            .getConstructor(String.class, String.class)
            .newInstance(
                volumeStr,
                bucketStr));
  }

  @SuppressFBWarnings("DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED")
  public static OzoneClientAdapter createAdapter(
      String volumeStr,
      String bucketStr,
      StorageStatistics storageStatistics) throws IOException {
    return createAdapter(volumeStr, bucketStr, false,
        (aClass) -> (OzoneClientAdapter) aClass
            .getConstructor(String.class, String.class,
                OzoneFSStorageStatistics.class)
            .newInstance(
                volumeStr,
                bucketStr,
                storageStatistics));
  }

  @SuppressFBWarnings("DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED")
  public static OzoneClientAdapter createAdapter(
      String volumeStr,
      String bucketStr,
      boolean basic,
      OzoneClientAdapterCreator creator) throws IOException {

    ClassLoader currentClassLoader =
        OzoneClientAdapterFactory.class.getClassLoader();
    List<URL> urls = new ArrayList<>();

    findEmbeddedLibsUrl(urls, currentClassLoader);

    findConfigDirUrl(urls, currentClassLoader);

    ClassLoader classLoader =
        new FilteredClassLoader(urls.toArray(new URL[0]), currentClassLoader);

    try {

      ClassLoader contextClassLoader =
          Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(classLoader);

      //this class caches the context classloader during the first load
      //call it here when the context class loader is set to the isoloated
      //loader to make sure the grpc class will be loaded by the right
      //loader
      Class<?> reflectionUtils =
          classLoader.loadClass("org.apache.ratis.util.ReflectionUtils");
      reflectionUtils.getMethod("getClassByName", String.class)
          .invoke(null, "org.apache.ratis.grpc.GrpcFactory");

      Class<?> adapterClass = null;
      if (basic) {
        adapterClass = classLoader
            .loadClass(
                "org.apache.hadoop.fs.ozone.BasicOzoneClientAdapterImpl");
      } else {
        adapterClass = classLoader
            .loadClass(
                "org.apache.hadoop.fs.ozone.OzoneClientAdapterImpl");
      }
      OzoneClientAdapter ozoneClientAdapter =
          creator.createOzoneClientAdapter(adapterClass);

      Thread.currentThread().setContextClassLoader(contextClassLoader);

      return ozoneClientAdapter;
    } catch (Exception e) {
      LOG.error("Can't initialize the ozoneClientAdapter", e);
      throw new IOException(
          "Can't initialize the OzoneClientAdapter implementation", e);
    }

  }

  private static void findConfigDirUrl(List<URL> urls,
      ClassLoader currentClassLoader) throws IOException {
    Enumeration<URL> conf =
        currentClassLoader.getResources("ozone-site.xml");
    while (conf.hasMoreElements()) {
      urls.add(
          new URL(
              conf.nextElement().toString().replace("ozone-site.xml", "")));

    }
  }

  private static void findEmbeddedLibsUrl(List<URL> urls,
      ClassLoader currentClassloader)
      throws MalformedURLException {

    //marker file is added to the jar to make it easier to find the URL
    // for the current jar.
    String markerFile = "ozonefs.txt";
    ClassLoader currentClassLoader =
        OzoneClientAdapterFactory.class.getClassLoader();

    URL ozFs = currentClassLoader
        .getResource(markerFile);
    String rootPath = ozFs.toString().replace(markerFile, "");
    urls.add(new URL(rootPath));

    urls.add(new URL(rootPath + "libs/"));

  }

  /**
   * Interface to create OzoneClientAdapter implementation with reflection.
   */
  @FunctionalInterface
  interface OzoneClientAdapterCreator {
    OzoneClientAdapter createOzoneClientAdapter(Class<?> clientAdapter)
        throws NoSuchMethodException, IllegalAccessException,
        InvocationTargetException, InstantiationException;
  }

}
