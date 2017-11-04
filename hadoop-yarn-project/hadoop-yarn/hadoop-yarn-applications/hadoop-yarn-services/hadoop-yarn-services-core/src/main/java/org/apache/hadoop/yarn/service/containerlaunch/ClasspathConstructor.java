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

package org.apache.hadoop.yarn.service.containerlaunch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * build a classpath -allows for entries to be injected in front of
 * YARN classpath as well as behind, adds appropriate separators, 
 * extraction of local classpath, etc.
 */
public class ClasspathConstructor {

    public static final String CLASS_PATH_SEPARATOR = ApplicationConstants.CLASS_PATH_SEPARATOR;
  private final List<String> pathElements = new ArrayList<>();

  public ClasspathConstructor() {
  }


  /**
   * Get the list of JARs from the YARN settings
   * @param config configuration
   */
  public List<String> yarnApplicationClasspath(Configuration config) {
    String[] cp = config.getTrimmedStrings(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH);
    return cp != null ? Arrays.asList(cp) : new ArrayList<String>(0);

  }


  @Override
  public String toString() {
    return buildClasspath();
  }

  public String buildClasspath() {
    return ServiceUtils.join(pathElements,
        CLASS_PATH_SEPARATOR,
        false);
  }

  /**
   * Get a copy of the path list
   * @return the JARs
   */
  public List<String> getPathElements() {
    return Collections.unmodifiableList(pathElements);
  }

  /**
   * Append an entry
   * @param path path
   */
  public void append(String path) {
    pathElements.add(path);
  }

  /**
   * Insert a path at the front of the list. This places it ahead of
   * the standard YARN artifacts
   * @param path path to the JAR. Absolute or relative -on the target
   * system
   */
  public void insert(String path) {
    pathElements.add(0, path);
  }

  public void appendAll(Collection<String> paths) {
    pathElements.addAll(paths);
  }

  public void insertAll(Collection<String> paths) {
    pathElements.addAll(0, paths);
  }


  public void addLibDir(String pathToLibDir) {
    append(buildLibDir(pathToLibDir));
  }

  public void insertLibDir(String pathToLibDir) {
    insert(buildLibDir(pathToLibDir));
  }

  public void addClassDirectory(String pathToDir) {
    append(appendDirectoryTerminator(pathToDir));
  }

  public void insertClassDirectory(String pathToDir) {
    insert(buildLibDir(appendDirectoryTerminator(pathToDir)));
  }


  public void addRemoteClasspathEnvVar() {
    append(ApplicationConstants.Environment.CLASSPATH.$$());
  }


  public void insertRemoteClasspathEnvVar() {
    append(ApplicationConstants.Environment.CLASSPATH.$$());
  }


  /**
   * Build a lib dir path
   * @param pathToLibDir path to the directory; may or may not end with a
   * trailing space
   * @return a path to a lib dir that is compatible with the java classpath
   */
  public String buildLibDir(String pathToLibDir) {
    String dir = appendDirectoryTerminator(pathToLibDir);
    dir += "*";
    return dir;
  }

  private String appendDirectoryTerminator(String pathToLibDir) {
    String dir = pathToLibDir.trim();
    if (!dir.endsWith("/")) {
      dir += "/";
    }
    return dir;
  }

  /**
   * Split a classpath. This uses the local path separator so MUST NOT
   * be used to work with remote classpaths
   * @param localpath local path
   * @return a splite
   */
  public Collection<String> splitClasspath(String localpath) {
    String separator = System.getProperty("path.separator");
    return StringUtils.getStringCollection(localpath, separator);
  }

  /**
   * Get the local JVM classpath split up
   * @return the list of entries on the JVM classpath env var
   */
  public Collection<String> localJVMClasspath() {
    return splitClasspath(System.getProperty("java.class.path"));
  }

}
