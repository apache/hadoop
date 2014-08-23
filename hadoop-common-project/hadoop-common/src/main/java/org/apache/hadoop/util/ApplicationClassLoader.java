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

package org.apache.hadoop.util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * A {@link URLClassLoader} for application isolation. Classes from the
 * application JARs are loaded in preference to the parent loader.
 */
@Public
@Unstable
public class ApplicationClassLoader extends URLClassLoader {
  /**
   * Default value of the system classes if the user did not override them.
   * JDK classes, hadoop classes and resources, and some select third-party
   * classes are considered system classes, and are not loaded by the
   * application classloader.
   */
  public static final String DEFAULT_SYSTEM_CLASSES =
        "java.," +
        "javax.," +
        "org.w3c.dom.," +
        "org.xml.sax.," +
        "org.apache.commons.logging.," +
        "org.apache.log4j.," +
        "org.apache.hadoop.," +
        "core-default.xml," +
        "hdfs-default.xml," +
        "mapred-default.xml," +
        "yarn-default.xml";

  private static final Log LOG =
    LogFactory.getLog(ApplicationClassLoader.class.getName());

  private static final FilenameFilter JAR_FILENAME_FILTER =
    new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar") || name.endsWith(".JAR");
      }
  };

  private final ClassLoader parent;
  private final List<String> systemClasses;

  public ApplicationClassLoader(URL[] urls, ClassLoader parent,
      List<String> systemClasses) {
    super(urls, parent);
    if (LOG.isDebugEnabled()) {
      LOG.debug("urls: " + Arrays.toString(urls));
      LOG.debug("system classes: " + systemClasses);
    }
    this.parent = parent;
    if (parent == null) {
      throw new IllegalArgumentException("No parent classloader!");
    }
    // if the caller-specified system classes are null or empty, use the default
    this.systemClasses = (systemClasses == null || systemClasses.isEmpty()) ?
        Arrays.asList(StringUtils.getTrimmedStrings(DEFAULT_SYSTEM_CLASSES)) :
        systemClasses;
    LOG.info("system classes: " + this.systemClasses);
  }

  public ApplicationClassLoader(String classpath, ClassLoader parent,
      List<String> systemClasses) throws MalformedURLException {
    this(constructUrlsFromClasspath(classpath), parent, systemClasses);
  }

  static URL[] constructUrlsFromClasspath(String classpath)
      throws MalformedURLException {
    List<URL> urls = new ArrayList<URL>();
    for (String element : classpath.split(File.pathSeparator)) {
      if (element.endsWith("/*")) {
        String dir = element.substring(0, element.length() - 1);
        File[] files = new File(dir).listFiles(JAR_FILENAME_FILTER);
        if (files != null) {
          for (File file : files) {
            urls.add(file.toURI().toURL());
          }
        }
      } else {
        File file = new File(element);
        if (file.exists()) {
          urls.add(new File(element).toURI().toURL());
        }
      }
    }
    return urls.toArray(new URL[urls.size()]);
  }

  @Override
  public URL getResource(String name) {
    URL url = null;
    
    if (!isSystemClass(name, systemClasses)) {
      url= findResource(name);
      if (url == null && name.startsWith("/")) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove leading / off " + name);
        }
        url= findResource(name.substring(1));
      }
    }

    if (url == null) {
      url= parent.getResource(name);
    }

    if (url != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getResource("+name+")=" + url);
      }
    }
    
    return url;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading class: " + name);
    }

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    if (c == null && !isSystemClass(name, systemClasses)) {
      // Try to load class from this classloader's URLs. Note that this is like
      // the servlet spec, not the usual Java 2 behaviour where we ask the
      // parent to attempt to load first.
      try {
        c = findClass(name);
        if (LOG.isDebugEnabled() && c != null) {
          LOG.debug("Loaded class: " + name + " ");
        }
      } catch (ClassNotFoundException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(e);
        }
        ex = e;
      }
    }

    if (c == null) { // try parent
      c = parent.loadClass(name);
      if (LOG.isDebugEnabled() && c != null) {
        LOG.debug("Loaded class from parent: " + name + " ");
      }
    }

    if (c == null) {
      throw ex != null ? ex : new ClassNotFoundException(name);
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }

  public static boolean isSystemClass(String name, List<String> systemClasses) {
    if (systemClasses != null) {
      String canonicalName = name.replace('/', '.');
      while (canonicalName.startsWith(".")) {
        canonicalName=canonicalName.substring(1);
      }
      for (String c : systemClasses) {
        boolean result = true;
        if (c.startsWith("-")) {
          c = c.substring(1);
          result = false;
        }
        if (c.endsWith(".") && canonicalName.startsWith(c)) {
          return result;
        } else if (canonicalName.equals(c)) {
          return result;
        }
      }
    }
    return false;
  }
}