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

package org.apache.hadoop.sqoop.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Allows you to add and remove jar-files from the running JVM by
 * instantiating classloaders for them.
 *
 * 
 *
 */
public final class ClassLoaderStack {

  public static final Log LOG = LogFactory.getLog(ClassLoaderStack.class.getName());

  private ClassLoaderStack() {
  }

  /**
   * Sets the classloader for the current thread
   */
  public static void setCurrentClassLoader(ClassLoader cl) {
    LOG.info("Restoring classloader: " + cl.toString());
    Thread.currentThread().setContextClassLoader(cl);
  }

  /**
   * Adds a ClassLoader to the top of the stack that will load from the Jar file
   * of your choice. Returns the previous classloader so you can restore it
   * if need be, later.
   *
   * @param jarFile The filename of a jar file that you want loaded into this JVM
   * @param tableClassName The name of the class to load immediately (optional)
   */
  public static ClassLoader addJarFile(String jarFile, String testClassName)
      throws IOException {

    // load the classes from the ORM JAR file into the current VM
    ClassLoader prevClassLoader = Thread.currentThread().getContextClassLoader();
    String urlPath = "jar:file://" + new File(jarFile).getAbsolutePath() + "!/";
    LOG.debug("Attempting to load jar through URL: " + urlPath);
    LOG.debug("Previous classloader is " + prevClassLoader);
    URL [] jarUrlArray = {new URL(urlPath)};
    URLClassLoader cl = URLClassLoader.newInstance(jarUrlArray, prevClassLoader);
    try {
      if (null != testClassName) {
        // try to load a class from the jar to force loading now.
        Class.forName(testClassName, true, cl);
      }
      LOG.info("Loaded jar into current JVM: " + urlPath);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load jar " + jarFile + " into JVM. (Could not find class "
          + testClassName + ".)", cnfe);
    }

    LOG.info("Added classloader for jar " + jarFile + ": " + cl);
    Thread.currentThread().setContextClassLoader(cl);
    return prevClassLoader;
  }
}
