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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

/**
 * Class loader which delegates the loading only for the selected class.
 *
 * <p>
 * By default java classloader delegates first all the class loading to the
 * parent, and loads the class only if it's not found in the class.
 * <p>
 * This simple class loader do the opposit. Everything is loaded with this
 * class loader without delegation _except_ the few classes which are defined
 * in the constructor.
 * <p>
 * With this method we can use two separated class loader (the original main
 * classloader and instance of this which loaded separated classes, but the
 * few selected classes are shared between the two class loaders.
 * <p>
 * With this approach it's possible to use any older hadoop version
 * (main classloader) together with ozonefs (instance of this classloader) as
 * only the selected classes are selected between the class loaders.
 */
public class FilteredClassLoader extends URLClassLoader {

  private final ClassLoader systemClassLoader;

  private final ClassLoader delegate;
  private Set<String> delegatedClasses = new HashSet<>();

  public FilteredClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, null);
    delegatedClasses.add("org.apache.hadoop.fs.ozone.OzoneClientAdapter");
    delegatedClasses.add("org.apache.hadoop.security.token.Token");
    delegatedClasses.add("org.apache.hadoop.fs.ozone.BasicKeyInfo");
    delegatedClasses.add("org.apache.hadoop.fs.ozone.OzoneFSOutputStream");
    delegatedClasses.add("org.apache.hadoop.fs.ozone.OzoneFSStorageStatistics");
    delegatedClasses.add("org.apache.hadoop.fs.ozone.Statistic");
    delegatedClasses.add("org.apache.hadoop.fs.Seekable");
    this.delegate = parent;
    systemClassLoader = getSystemClassLoader();

  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    if (delegatedClasses.contains(name) ||
        name.startsWith("org.apache.log4j") ||
        name.startsWith("org.slf4j")) {
      return delegate.loadClass(name);
    }
    return super.loadClass(name);
  }

  private Class<?> loadFromSystem(String name) {
    if (systemClassLoader != null) {
      try {
        return systemClassLoader.loadClass(name);
      } catch (ClassNotFoundException ex) {
        //no problem
        return null;
      }
    } else {
      return null;
    }
  }
}
