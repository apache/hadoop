/*
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

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class ClassUtil {
  /** 
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * 
   * @param clazz the class to find.
   * @return a jar file that contains the class, or null.
   */
  public static String findContainingJar(Class<?> clazz) {
    return findContainingResource(clazz.getClassLoader(), clazz.getName(), "jar");
  }

  /**
   * Find the absolute location of the class.
   *
   * @param clazz the class to find.
   * @return the class file with absolute location, or null.
   */
  public static String findClassLocation(Class<?> clazz) {
    return findContainingResource(clazz.getClassLoader(), clazz.getName(), "file");
  }

  private static String findContainingResource(ClassLoader loader, String clazz, String resource) {
    String classFile = clazz.replaceAll("\\.", "/") + ".class";
    try {
      for (final Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements();) {
        final URL url = itr.nextElement();
        if (resource.equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
