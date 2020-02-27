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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Helper class to start a new process.
 */
public class JavaProcess {

  private Process process = null;

  public JavaProcess(Class<?> clazz, File output)
      throws IOException, InterruptedException {
    this(clazz, null, output);
  }

  public JavaProcess(Class<?> clazz, List<String> addClasspaths, File output)
      throws IOException, InterruptedException {
    String javaHome = System.getProperty("java.home");
    String javaBin =
        javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    classpath = classpath.concat("./src/test/resources");
    if (addClasspaths != null) {
      for (String addClasspath : addClasspaths) {
        classpath = classpath.concat(File.pathSeparatorChar + addClasspath);
      }
    }
    String className = clazz.getCanonicalName();
    ProcessBuilder builder =
        new ProcessBuilder(javaBin, "-cp", classpath, className);
    builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
    builder.redirectOutput(output);
    builder.redirectError(output);
    process = builder.start();
  }

  public void stop() throws InterruptedException {
    if (process != null) {
      process.destroy();
      process.waitFor();
      process.exitValue();
    }
  }

}